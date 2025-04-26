import os
import sys
import string
import re

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import udf, current_timestamp, date_format
from pyspark.sql.types import StringType, FloatType, StructType, StructField, BooleanType, IntegerType, ArrayType, DoubleType, MapType
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, MinHashLSH
from pyspark.ml.linalg import Vectors, VectorUDT
import underthesea
import numpy as np

# CASTING FUNCTIONS
def cast_to_string(value):
    try:
        return str(value)
    except (ValueError, TypeError):
        return None

def cast_to_boolean(value):
    try:
        return bool(value)
    except (ValueError, TypeError):
        return None

def cast_to_integer(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None
    
def cast_to_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
    
# DUPLICATION PROCESSING FUNCTIONS
@udf(returnType=VectorUDT())
def append_non_zero_to_vector(vector, append_value=0.1):
    new_vector_dim = len(vector) + 1
    new_vector_indices = list(vector.indices) + [len(vector)]
    new_vector_values = list(vector.values) + [append_value]
    new_vector = Vectors.sparse(new_vector_dim,
                                new_vector_indices,
                                new_vector_values)
    return new_vector

def get_text_tfidf_vectors(df):
    df = df.withColumn("text", f.concat_ws(" ", f.col("title"), f.col("description"), f.col("address.full_address")))

    # Calculate TF-IDF vectors
    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
    df = tokenizer.transform(df)
    hashingTF = HashingTF(inputCol="tokens", outputCol="tf")
    df = hashingTF.transform(df)
    idf = IDF(inputCol="tf", outputCol="tfidf")
    idf_model = idf.fit(df)
    df = idf_model.transform(df)
    # Append non-zero value to vectors
    df = df.withColumn("text_vector", append_non_zero_to_vector(f.col("tfidf"), f.lit(0.1)))
    return df.drop("text", "tokens", "tf", "tfidf")

def get_duplicate_df_with_minhash(df, threshhold=0.5, num_hash_tables=3, dist_col="distCol"):
    # df must already have "id" column
    minhashLSH = MinHashLSH(inputCol="text_vector", outputCol="hashes", numHashTables=num_hash_tables)
    model = minhashLSH.fit(df)
    duplicate_df = model.approxSimilarityJoin(df.select("id", "text_vector"), df.select("id", "text_vector"), 0.8, distCol=dist_col) \
                         .filter("datasetA.id < datasetB.id")  # Avoid comparing a row to itself
    duplicate_df = duplicate_df.withColumn("id", f.col("datasetA.id")) \
                               .withColumn("duplicate_with_id", f.col("datasetB.id")) \
                               .select("id", "duplicate_with_id", dist_col)
    return duplicate_df

def remove_duplicated_rows(df, remove_ids):
    # df must already have "id" column
    remove_ids = remove_ids.select("id")
    result_df = df.join(remove_ids, df["id"] == remove_ids["id"], "leftanti")
    return result_df

# TEXT PROCESSING FUNCTIONS
def get_special_chars(df: pyspark.sql.dataframe.DataFrame):
    # get concatenated text
    concatenated_text = df.select(f.concat_ws(' ', f.col('title'), f.col('description')).alias('concatenated_text'))
    all_characters = concatenated_text.rdd.flatMap(lambda x: x[0])
    special_characters = all_characters.filter(lambda c: not c.isalnum() and not c.isspace() and not c in string.punctuation)
    return set(special_characters.collect())

def get_estate_types(df: pyspark.sql.dataframe.DataFrame):
    df = df.filter(df['estate_type'].isNotNull())
    all_estate_types = df.select('estate_type').rdd.map(lambda x: x[0])
    estate_types_set = set(all_estate_types.collect())
    return estate_types_set

@udf(returnType=StringType())
def remove_special_chars(input_string, special_chars_list, at_once=False):
    if not input_string:
        return None
    if at_once:
        special_chars_string = ''.join(special_chars_list)
        translator = str.maketrans('', '', special_chars_string)
        result = input_string.translate(translator)
    else:
        result = input_string
        for c in special_chars_list:
            result = result.replace(c, '')
    return result

@udf(returnType=StringType())
def remove_duplicate_punctuation_sequence(input_string):
    def remove_duplicate_sequence(text, target_char, max_length):
        pattern_1 = re.escape(target_char) + '{' + str(max_length) + ',}'
        pattern_2 = '(' + '\s' + re.escape(target_char) + ')' + '{' + str(max_length) + ',}'
        result = re.sub(pattern_2, target_char, re.sub(pattern_1, target_char, text))
        return result
    
    if not input_string:
        return None
    result = input_string
    for punc in string.punctuation:
        if punc == '\\':
            continue
        max_length = 3 if punc == '.' else 1
        result = remove_duplicate_sequence(result, punc, max_length)
    return result

@udf(returnType=StringType())
def normalize_estate_type(input_estate_type):
    if not input_estate_type:
        return None
    estate_type_prefix = ['Cho thuể', 'Mua bán', 'Căn hộ']
    estate_type_map = {
        'Biệt thự, liền k`ề': 'Biệt thự liền kề',
        'Nhà biệt thự liền kề': 'Biệt thự liền kề',
        'Nhà mặt phố': 'Nhà mặt tiền',
        'Phòng trọ, nhà trọ, nhà trọ': 'Phòng trọ, nhà trọ',
        'Phòng trọ': 'Phòng trọ, nhà trọ',
        'Trang trại, khu nghỉ dưỡng': 'Trang trại khu nghỉ dưỡng',
        'Kho nhà xưởng': 'Kho xưởng',
        'Kho, xưởng': 'Kho xưởng'
    }
    result = input_estate_type
    for prefix in estate_type_prefix:
        result = result.replace(prefix, '').strip().capitalize()
    for estate_type in estate_type_map.keys():
        if result == estate_type:
            result = estate_type_map[estate_type]
    return result

# NUMBERS PROCESSING FUNCTION
def get_lower_upper_bound(df, col_name, lower_percent=5, upper_percent=95, outlier_threshold=5):
    lower_percentile, upper_percentile = df.approxQuantile(col_name, [lower_percent/100, upper_percent/100], 0.01)
    quantile_range = upper_percentile - lower_percentile
    lower_bound = np.max([0, lower_percentile - outlier_threshold * quantile_range])
    upper_bound = upper_percentile + outlier_threshold * quantile_range
    return lower_bound, upper_bound

def get_detail_lower_upper_bound(df, col_name, lower_percent=5, upper_percent=95, outlier_threshold=5):
    quantiles_by_estate_type = (
        df.groupBy("estate_type")
        .agg(f.percentile_approx(col_name, [lower_percent/100, upper_percent/100], 100).alias("percentile_approx"))
    )
    quantiles_by_estate_type = quantiles_by_estate_type.withColumn("lower_percentile", f.col("percentile_approx").getItem(0)) \
                                                       .withColumn("upper_percentile", f.col("percentile_approx").getItem(1)) \
                                                       .withColumn("quantile_range", f.col("upper_percentile") - f.col("lower_percentile"))
    quantiles_by_estate_type = quantiles_by_estate_type.withColumn("lower_bound", f.greatest(f.col("lower_percentile") - outlier_threshold * f.col("quantile_range"), f.lit(0))) \
                                                       .withColumn("upper_bound", f.col("upper_percentile") + outlier_threshold * f.col("quantile_range"))
    
    return quantiles_by_estate_type.select("estate_type", "lower_bound", "upper_bound")

def filter_with_detail_bound(df, bound_df, join_col_name, filter_col_name):
    join_df = df.join(bound_df.alias("bound_df"), join_col_name, "inner")
    filtered_df = join_df.filter((join_df[filter_col_name] >= join_df["lower_bound"]) \
                                 & (join_df[filter_col_name] <= join_df["upper_bound"]))
    return filtered_df.drop("lower_bound", "upper_bound")

@udf(returnType=FloatType())
def price_normalize(price, square):
    if price is None:
        return None
    if isinstance(price, int) or isinstance(price, float):
        return price
    elif isinstance(price, str):
        if cast_to_float(price) is not None:
            return cast_to_float(price)
        if square is not None:
            price = underthesea.text_normalize(price)
            # Các trường hợp thực sự điền giá / m2
            if 'triệu/ m' in price or 'triệu / m' in price:
                price = float(price.split()[0]) * 1e6 * square
            # Các trường hợp điền nhầm giá sang giá / m2
            elif 'tỷ/ m' in price or 'tỷ / m' in price:
                price = float(price.split()[0]) * 1e9
            else:
                price = None
        elif square is None:
            price = None
    return price

# EXTRA INFOS PROCESSING FUNCTIONS
def get_extra_info_labels(df):
    extra_infos_df = df.select("extra_infos")
    extra_infos_labels = extra_infos_df.rdd.flatMap(lambda x: list(x[0].asDict().keys())).collect()
    return set(extra_infos_labels)
    
def normalize_text_field_in_dict(dict_obj):
    result_dict = dict_obj
    for key in result_dict.keys():
        if isinstance(result_dict[key], str):
            result_dict[key] = result_dict[key].replace(',', '.')
            new_val = ''
            for c in result_dict[key]:
                if c.isalpha() or c.isnumeric() or c == '.' or c == ' ':
                    new_val += c
            result_dict[key] = new_val
    return result_dict

# @udf(returnType=StructType([
#     StructField('Chiều dài', FloatType()),
#     StructField('Chiều ngang', FloatType()),
#     StructField('Chính chủ', BooleanType()),
#     StructField('Chỗ để xe hơi', BooleanType()),
#     StructField('Hướng', StringType()),
#     StructField('Lộ giới', FloatType()),
#     StructField('Nhà bếp', BooleanType()),
#     StructField('Pháp lý', StringType()),
#     StructField('Phòng ăn', BooleanType()),
#     StructField('Sân thượng', BooleanType()),
#     StructField('Số lầu', IntegerType()),
#     StructField('Số phòng ngủ', IntegerType()),
#     StructField('Số toilet', IntegerType()),
#     StructField('Tầng', IntegerType()),
# ]))
@udf(returnType=StructType([
    StructField('no_bedrooms', IntegerType()),
    StructField('no_bathrooms', IntegerType()),
    StructField('front_road', FloatType()),
    StructField('front_face', FloatType()),
    StructField('no_floors', IntegerType()),
    StructField('direction', StringType()),
    StructField('ultilization_square', FloatType()),
    StructField('yo_construction', IntegerType()),
]))
def normalize_extra_infos_dict(input_extra_infos_row, old_keys, new_keys, remove_keys):
    if input_extra_infos_row is None:
        return None
    old_keys = list(old_keys)
    new_keys = list(new_keys)
    remove_keys = list(remove_keys)

    # Normalize dict keys
    extra_infos_dict = input_extra_infos_row  # input_extra_infos_row đã là một dict từ MapType
    dict_normalized_keys = {k: None for k in new_keys}

    # Define mapping of possible old keys to new keys
    key_mapping = {
        'no_bedrooms': ['Số Phòng Ngủ', 'Số phòng ngủ :'],
        'no_bathrooms': ['Số Phòng Tắm', 'Số toilet :'],
        'front_road': ['Đường Trước Nhà'],
        'front_face': ['Mặt Tiền'],
        'no_floors': ['Số Tầng', 'Tầng :'],
        'direction': ['Hướng Nhà'],
        'ultilization_square': ['Diện Tích Sử Dụng'],
        'yo_construction': ['Năm xây dựng']
    }

    # Assign values from possible old keys to new keys
    for new_key, possible_old_keys in key_mapping.items():
        for old_key in possible_old_keys:
            if old_key in extra_infos_dict.keys() and dict_normalized_keys[new_key] is None:
                dict_normalized_keys[new_key] = extra_infos_dict[old_key]
                break  # Stop after finding the first match

    # Remove unwanted keys
    for key in remove_keys:
        if key in dict_normalized_keys.keys():
            dict_normalized_keys.pop(key)

    # Normalize dict values
    result_dict = normalize_text_field_in_dict(dict_normalized_keys)
    
    # Type casting with English field names
    result_dict['no_bedrooms'] = cast_to_integer(result_dict['no_bedrooms']) if result_dict['no_bedrooms'] is not None else None
    result_dict['no_bathrooms'] = cast_to_integer(result_dict['no_bathrooms']) if result_dict['no_bathrooms'] is not None else None
    result_dict['front_road'] = cast_to_float(result_dict['front_road'].replace('mm', '').replace('m', '')) if result_dict['front_road'] is not None else None
    result_dict['front_face'] = cast_to_float(result_dict['front_face'].replace('mm', '').replace('m', '')) if result_dict['front_face'] is not None else None
    result_dict['no_floors'] = cast_to_integer(result_dict['no_floors']) if result_dict['no_floors'] is not None else None
    result_dict['direction'] = cast_to_string(result_dict['direction']) if result_dict['direction'] is not None else None
    result_dict['ultilization_square'] = cast_to_float(result_dict['ultilization_square'].replace('m2', '')) if result_dict['ultilization_square'] is not None else None
    result_dict['yo_construction'] = cast_to_integer(result_dict['yo_construction']) if result_dict['yo_construction'] is not None else None
    
    return result_dict

# MAIN SPARK JOB
spark = SparkSession.builder \
    .appName("StreamProcessing") \
    .config("spark.jars", "/opt/bitnami/spark/jars/elasticsearch-spark-30_2.12-8.11.0.jar,/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,/opt/bitnami/spark/jars/spark-streaming-kafka-0-10_2.12-3.5.0.jar,/opt/bitnami/spark/jars/kafka-clients-2.8.0.jar,/opt/bitnami/spark/jars/commons-pool2-2.11.1.jar,/opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,/opt/bitnami/spark/jars/commons-httpclient-3.1.jar") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.streaming.kafka.maxRatePerPartition", "100") \
    .getOrCreate()
server_ip = "35.226.11.119"
# Set up data stream
kafka_config = {
    "kafka.bootstrap.servers": f"{server_ip}:9192,{server_ip}:9292,{server_ip}:9392",
    "subscribe": "nhapho,bietthu,chungcu,nharieng",  # Subscribe 4 topic
    "startingOffsets": "earliest"
}
streaming_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_config["kafka.bootstrap.servers"]) \
  .option("subscribe", kafka_config["subscribe"]) \
  .option("startingOffsets", kafka_config["startingOffsets"]) \
  .load()

schema = StructType([
    StructField("address", StructType([
        StructField("district", StringType(), nullable=True),
        StructField("full_address", StringType(), nullable=True),
        StructField("province", StringType(), nullable=True),
        StructField("ward", StringType(), nullable=True),
    ]), nullable=True),
    StructField("contact_info", StructType([
        StructField("name", StringType(), nullable=True),
        StructField("phone", ArrayType(StringType()), nullable=True),
    ]), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("estate_type", StringType(), nullable=True),
    StructField("extra_infos", MapType(StringType(), StringType()), nullable=True),
    StructField("link", StringType(), nullable=True),
    StructField("post_date", StringType(), nullable=True),
    StructField("post_id", StringType(), nullable=True),
    StructField("price", StringType(), nullable=True),
    StructField("square", DoubleType(), nullable=True),
    StructField("title", StringType(), nullable=True),
])

json_string_df = streaming_df.selectExpr("CAST(value AS STRING) as json_string", "topic")
# Use the predefined schema from collected data
parsed_json_df = json_string_df.select(
    f.from_json("json_string", schema).alias("data"), "topic"
)
final_df = parsed_json_df.select("data.*", "topic")

# Text processing
special_chars_list = ['→', '\u202a', '\uf0d8', '✤', '\u200c', 'ۣ', '🅖', '–', '₋', '●', '¬', '̶', '▬', '≈', '🫵', '◇', '▷', '🪷', '◊', '‐', '🫴', '\uf05b', '⦁', '️', '㎡', '🫰', '′', '✥', '✧', '♤', '🫶', 'ۜ', '❃', '̀', '֍', '\u2060', '\u206e', '‘', '❈', '🅣', '🅘', '℅', '\ufeff', '″', '\u200b', '♚', '̣', '₫', '\uf06e', '✩', '🅨', '’', '\xad', '★', '±', '\U0001fae8', '︎', '\uf0f0', '∙', '♛', '̉', '̛', '❆', '✜', '÷', '♜', '·', '❖', '】', '❁', '🫱', '・', '€', '☛', '“', '■', '\uf046', '￼', '�', '\u200d', '🫠', '\uf0e8', '⁃', '≥', '～', '➣', '́', '🪩', '̃', '\uf02b', '᪥', '🪺', '♧', '❂', '。', '♡', '，', '🪸', '：', '¥', '❝', '̂', '\U0001fa77', '\uf0a7', 'ৣ', '⚘', '➢', '⇔', '、', '－', '✆', '🫣', '⛫', '►', '̆', '✎', '❯', '《', '\uf076', '❮', '❀', '̵', '🥹', '❉', '̷', '\uf028', '✽', '«', '⇒', '➤', '\uf0e0', '\U0001faad', '♙', '\uf0fc', '【', '➥', '¤', '＆', '🛇', '\x7f', '）', '—', '”', '❞', '》', '☆', '×', '✞', '✿', '≤', '🅐', '√', '°', '✓', '¡', '…', '•', '»', '❊', '➦', '\u06dd', '\uf06c', '¸']
final_df = final_df.withColumn("title", remove_special_chars("title", f.lit(special_chars_list)))
final_df = final_df.withColumn("description", remove_special_chars("description", f.lit(special_chars_list)))
final_df = final_df.withColumn("title", remove_duplicate_punctuation_sequence("title"))
final_df = final_df.withColumn("description", remove_duplicate_punctuation_sequence("description"))
final_df = final_df.withColumn("estate_type", normalize_estate_type("estate_type"))
print("Text processed.")

# Numbers processing
final_df = final_df.withColumn("price/square", f.col("price")/f.col("square"))
final_df = final_df.withColumn("price", price_normalize("price", "square"))
print("Numbers processed.")

# Extra infos processing
old_keys = [
    'Số Phòng Ngủ', 'Số Phòng Tắm', 'Đường Trước Nhà', 'Mặt Tiền', 'Số Tầng', 
    'Hướng Nhà', 'Diện Tích Sử Dụng', 'Năm xây dựng',  # Nguồn 1
    'Số phòng ngủ :', 'Số toilet :', 'Tầng :'  # Nguồn 2
]
new_keys = [
    'no_bedrooms', 'no_bathrooms', 'front_road', 'front_face', 'no_floors', 
    'direction', 'ultilization_square', 'yo_construction',
    'no_bedrooms', 'no_bathrooms', 'no_floors'  # Ánh xạ tương ứng
]
remove_keys = []
final_df = final_df.withColumn("extra_infos", normalize_extra_infos_dict("extra_infos", f.lit(old_keys), f.lit(new_keys), f.lit(remove_keys)))
print("Extra infos processed.")

final_df = final_df.withColumn("created_at", date_format(current_timestamp(), "yyyy/MM/dd HH:mm:ss"))
print("Added created_at field.")

# Output two queries, one to console and one to Elasticsearch
es_config_base = {
    "es.nodes": f"{server_ip}",
    "es.port": "9200",
    "es.nodes.wan.only": "true",
    "es.nodes.discovery": "false",
    "es.batch.write.retry.count": "3",
    "es.index.auto.create": "true",
    "es.spark.sql.streaming.sink.log.enabled": "true"
}

# Định nghĩa ánh xạ topic và index
topics_indices = {
    "nhapho": "nhapho_index",
    "bietthu": "bietthu_index",
    "chungcu": "chungcu_index",
    "nharieng": "nharieng_index"
}

# Khởi động các luồng ghi
queries = []

# Ghi ra console
console_query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
queries.append(console_query)

# Ghi vào Elasticsearch theo topic
for topic, index in topics_indices.items():
    topic_df = final_df.filter(f.col("topic") == topic)
    es_query = topic_df.writeStream \
        .outputMode("append") \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", es_config_base["es.nodes"]) \
        .option("es.port", es_config_base["es.port"]) \
        .option("es.resource", index) \
        .option("es.nodes.wan.only", es_config_base["es.nodes.wan.only"]) \
        .option("es.index.auto.create", es_config_base["es.index.auto.create"]) \
        .option("es.nodes.discovery", es_config_base["es.nodes.discovery"]) \
        .option("es.batch.write.retry.count", es_config_base["es.batch.write.retry.count"]) \
        .option("es.spark.sql.streaming.sink.log.enabled", es_config_base["es.spark.sql.streaming.sink.log.enabled"]) \
        .option("checkpointLocation", f"/tmp/checkpoint/{topic}") \
        .start()
    queries.append(es_query)

# Chờ bất kỳ luồng nào hoàn thành
try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    print(f"Error in streaming: {e}")
finally:
    spark.stop()