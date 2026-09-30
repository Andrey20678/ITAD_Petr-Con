"""Скачать небольшой HTTP-файл и сохранить его без изменений в raw."""

from __future__ import annotations

import logging
from typing import Any
from io import BytesIO

import requests
import zipfile

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

RAW_BUCKET = "raw"


def load_to_raw(
    *,
    source_url: str,
    source_filename: str,
    source_file_content: list[str] | None,
    source_file_password: bytes | None,
    dataset_slug: str,
    s3_conn_id: str,
    **context: Any,
) -> None:
    """
    Загрузить исходный файл или оставить уже созданный raw-объект.
    Если исходный файл это zip архив и указан source_file_content,
    то будут извлечены и загружены указанные в нём файлы
    """

    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    if not s3_hook.check_for_bucket(bucket_name=RAW_BUCKET):
        s3_hook.create_bucket(bucket_name=RAW_BUCKET)
        logging.info("Created raw bucket: s3://%s", RAW_BUCKET)

    
    keys = []
    if source_file_content:
        for s in source_file_content:
            key = f"{dataset_slug}/ingested_on={context['ds']}/{s}"
            if not check_key(s3_hook, key):
                keys.append((s, key))
    else:
        key = f"{dataset_slug}/ingested_on={context['ds']}/{source_filename}"
        if not check_key(s3_hook, key):
            keys.append((s, key))

    if len(keys) == 0:
        return

    response = requests.get(source_url, timeout=60)
    response.raise_for_status()

    content = BytesIO(response.content)

    if source_file_content:
        if zipfile.is_zipfile(content):
            zip_data = zipfile.ZipFile(content, "r")
            for s, key in keys:
                load_bytes(s3_hook, zip_data.read(s, source_file_password), key, f"{source_url} {s}")
            zip_data.close()
            return
    load_bytes(s3_hook, content, source_filename, source_url)


def check_key(s3_hook: S3Hook, key):
    if s3_hook.check_for_key(key=key, bucket_name=RAW_BUCKET):
        logging.info(
            "Raw object already exists and will not be modified: s3://%s/%s",
            RAW_BUCKET,
            key,
        )
        return True
    return False


def load_bytes(s3_hook: S3Hook, content: bytes, key: str, source_url: str):
    s3_hook.load_bytes(
            bytes_data=content,
            key=key,
            bucket_name=RAW_BUCKET,
            replace=False,
        )
    logging.info("Downloaded HTTP file %s to s3://%s/%s", source_url, RAW_BUCKET, key)