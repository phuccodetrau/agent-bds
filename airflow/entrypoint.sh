#!/bin/bash

# Khởi tạo cơ sở dữ liệu Airflow nếu chưa có
airflow db init

# Tạo tài khoản admin nếu chưa tồn tại
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

# Chạy lệnh được truyền vào CMD (mặc định là webserver)
exec "$@"