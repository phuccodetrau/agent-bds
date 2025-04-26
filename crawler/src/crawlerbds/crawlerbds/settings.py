# Scrapy settings for batdongsan project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "crawlerbds"

SPIDER_MODULES = ["crawlerbds.spiders"]
NEWSPIDER_MODULE = "crawlerbds.spiders"

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36"
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
]
# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Spash server endpoint
#

ITEM_PIPELINES = {
    'crawlerbds.pipelines.TextNormalizePipeline': 100,
    'crawlerbds.pipelines.AddressCorretionPipeline': 200,
    'crawlerbds.pipelines.FieldsPreprocessPipeline': 300,
    'crawlerbds.pipelines.DuplicateCheckPipeline': 400,
    'crawlerbds.pipelines.PushToKafka': 500,
}

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 16

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 5
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = True

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
    # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    # "Accept-Language": "en-US,en;q=0.5",
    # "Accept-Encoding": "gzip, deflate, br",
    # "Connection": "keep-alive",
}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
    #    "batdongsan.middlewares.BatdongsanSpiderMiddleware": 543,

    # Enable Splash Deduplicate Args Filter
    # 'scrapy_splash.SplashDeduplicateArgsMiddleware': 100,
}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html

# Spash Server Endpoint
# SPLASH_URL = 'http://0.0.0.0:8050/'

DOWNLOADER_MIDDLEWARES = {
    #    "batdongsan.middlewares.BatdongsanDownloaderMiddleware": 543,

    # Rotating User Agents
    #    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    #    'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,

    # 'scrapy_tls_client.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': None,
    # 'scrapy_tls_client.downloaderMiddleware.TlsClientDownloaderMiddleware': 543,

    # The priority of 560 is important, because we want this middleware to kick in just before the scrapy built-in `RetryMiddleware`.
    # 'crawlerbds.middlewares.UARotatorMiddleware': 400,
    'scrapy_cloudflare_middleware.middlewares.CloudFlareMiddleware': 560,
    "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler": 570,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 580,
    # 'rotating_proxies.middlewares.RotatingProxyMiddleware': 300,
    # 'rotating_proxies.middlewares.BanDetectionMiddleware': 310,

    ## Rotating Free Proxies
    #    'scrapy_proxy_pool.middlewares.ProxyPoolMiddleware': 610,
    #    'scrapy_proxy_pool.middlewares.BanDetectionMiddleware': 620,

    # Enable Splash downloader middleware and change HttpCompressionMiddleware priority
    # 'scrapy_splash.SplashCookiesMiddleware': 723,
    # 'scrapy_splash.SplashMiddleware': 725,
    # 'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
}
# ROTATING_PROXY_LIST_PATH = "proxies.txt"
# ROTATING_PROXY_PAGE_RETRY_TIMES = 1  # Mặc định là 5, giảm xuống để thử nhanh hơn
# ROTATING_PROXY_BACKOFF_BASE = 5  # Giảm thời gian backoff (mặc định là 10)
# ROTATING_PROXY_BACKOFF_CAP = 30  # Giới hạn thời gian backoff tối đa (mặc định là 300)
# ROTATING_PROXY_CLOSE_SPIDER = False  # Không dừng Spider khi hết proxy


# Define the Splash DupeFilter
# DUPEFILTER_CLASS = 'scrapy_splash.SplashAwareDupeFilter'

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#    "batdongsan.pipelines.BatdongsanPipeline": 300,
# }

DOWNLOAD_TIMEOUT = 600  # Set the timeout to 300 seconds (default = 180)
DOWNLOAD_DELAY = 2  # Set the delay between requests to 0.5 seconds (default = 0)

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = "httpcache"
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

LOG_FILE_APPEND = False

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# Tls_client
CLIENT_IDENTIFIER = 'chrome_112'
RANDOM_TLS_EXTENSION_ORDER = True
FORCE_HTTP1 = False  # default False
CATCH_PANICS = False  # default False
RAW_RESPONSE_TYPE = 'HtmlResponse'  # HtmlResponse or TextResponse, default HtmlResponse