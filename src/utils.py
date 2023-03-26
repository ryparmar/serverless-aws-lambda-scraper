import argparse
import json
import logging
import os
import posixpath
import urllib.parse
from io import BytesIO
from typing import Dict, List, Set, Union

import boto3
import requests
from google.cloud import storage
from PIL import Image

from src.config import (GCP_PROJECT, GCS_STORAGE, HOME_URL, ITEM_DATA_ALL,
                        S3_BUCKET)

# Setup module logger
logger = logging.getLogger(__name__)


# File operations
def append_urls_to_file(filepath: str, content: Union[List[str], Set[str]]) -> None:
    """Save list or set of urls into a given file. Single url per line."""
    with open(filepath, "a", encoding="utf-8") as fw:
        fw.write("\n".join(content))


def read_urls_from_file(filepath: str) -> List[str]:
    """Read urls from a given file. Single url per line."""
    logger.info("Reading item_urls from local file.")
    with open(filepath, "r", encoding="utf-8") as fr:
        return [line.strip() for line in fr.readlines()]


def append_json_to_jsonl_file(output_file: str, data: Dict[str, str]) -> None:
    """Append given data to the file"""
    with open(output_file, "a", encoding="utf-8") as fw:
        fw.write(json.dumps(data, ensure_ascii=False) + "\n")


def download_images(img_urls: List[str], filepath: str) -> None:
    """Download images from list of urls"""
    for i, img_url in enumerate(img_urls):
        # Request the image and open it
        r = requests.get(img_url)
        img = Image.open(BytesIO(r.content))
        # Save image
        img.save(f"{filepath}_{i}.png")


def read_item_ids_from_jsonl(file: str) -> Set[str]:
    assert file.endswith(".jsonl")

    with open(file) as fr:
        return {json.loads(line)["id"] for line in fr}


# URLs operations
def item_url_to_path(item_url: str, home_url: str = HOME_URL) -> str:
    """Converts item url to path like string

    Example:
        In: https://www.vinted.cz/zeny/obleceni/saty/mini-saty/2353299058-deezee-bezove-saty
        Out: zeny/obleceni/saty/mini-saty
    """
    return "/".join(item_url.replace(home_url, "").split("/")[:-1])


def item_url_to_img_path(item_url: str, prefix: str = "data/item_data/images", numbered: bool = False) -> str:
    """Convert item_url to expected image path"""
    item_id = item_url.split("/")[-1].split("-")[0]
    img_filepath = os.path.join(prefix, item_url_to_path(item_url, HOME_URL), f"{item_id}.png")
    return img_filepath.replace(".png", "_0.png") if numbered else img_filepath


def construct_starting_urls(args: argparse.Namespace) -> List[str]:
    """Construct the initial pages for scraping"""
    return [urllib.parse.urljoin(HOME_URL, posixpath.join(category, "obleceni")) for category in args.categories]


def remove_duplicate_urls_from_file(filepath: str) -> None:
    urls = read_urls_from_file(filepath)
    unique_urls = set(urls)
    with open(filepath, "w") as fw:
        fw.write("\n".join(unique_urls))
    logger.info(f"Removed duplicates from the file with urls. Before {len(urls)} - after {len(unique_urls)}")


def filter_already_scraped_item_urls(item_urls: List[str]) -> List[str]:
    logger.info("Filtering out already scraped items.")
    scraped_item_ids = gcs_read_item_ids_from_jsonl(ITEM_DATA_ALL)
    logger.info(f"Found {len(scraped_item_ids)} already scraped items")

    non_scraped_item_urls = []
    for item_url in item_urls:
        item_id = item_url.split("/")[-1].split("-")[0]
        # item doesn't have image or data scraped
        if not gcs_item_url_img_exists(item_url) or not (item_id in scraped_item_ids):
            non_scraped_item_urls.append(item_url)
    return non_scraped_item_urls


# GCS operations
def gcs_upload_file(source_file_name: str, destination_file_name: str):
    """Uploads a file to the GCS bucket"""
    storage_client = storage.Client(GCP_PROJECT)
    bucket = storage_client.bucket(GCS_STORAGE)
    blob = bucket.blob(destination_file_name)
    blob.upload_from_filename(source_file_name)
    logger.info(f"File {source_file_name} uploaded to {destination_file_name}.")


def gcs_upload_file_from_variale(source_variable: str, destination_file_name: str):
    """Uploads variable to a file in the GCS bucket"""
    storage_client = storage.Client(GCP_PROJECT)
    bucket = storage_client.bucket(GCS_STORAGE)
    blob = bucket.blob(destination_file_name)
    blob.upload_from_string(source_variable)
    logger.info(f"{destination_file_name} uploaded to {destination_file_name}.")


def gcs_get_categories_from_item_url_files(site: str) -> List[str]:
    """Returns list of categories (e.g. ['muzi', 'zeny'] extracted from directory and file structure in GCS"""
    storage_client = storage.Client(GCP_PROJECT)
    prefix = f"data/item_urls/{site}/"
    url_files = [i.name.replace(prefix, "") for i in storage_client.list_blobs(GCS_STORAGE, prefix=prefix)]
    categories = list(set(map(lambda x: x.split("/")[0], url_files)))
    return categories


def gcs_get_all_item_url_filepaths(site: str, categories: List[str]) -> List[str]:
    """Returns all item_url filepaths for given site and category"""
    logger.info(f"Retrieving all item_url files for {site} {categories}.")
    assert isinstance(categories, list), f"Categories are expected to be a list and not a {type(categories)}."
    storage_client = storage.Client(GCP_PROJECT)
    all_file_names = []
    for category in categories:
        prefix = f"data/item_urls/{site}/{category}/"
        all_file_names.extend([i.name for i in storage_client.list_blobs(GCS_STORAGE, prefix=prefix)])
    logger.info(f"Retrieved filepaths: {all_file_names}.")
    return all_file_names


def gcs_get_last_item_url_filepath(site: str, categories: List[str]) -> List[str]:
    """Returns last item_url filepath for given site and category"""
    logger.info(f"Retrieving last item_url files for {site} {categories}.")
    latest_files = []
    for category in categories:
        prefix = f"data/item_urls/{site}/{category}/"
        file_names = gcs_get_all_item_url_filepaths(site, [category])
        latest_datetime = sorted([i.split("/")[-1].split("_")[2].rstrip(".txt") for i in file_names])[-1]
        latest_files.append(os.path.join(prefix, f"item_urls_{latest_datetime}.txt"))
    logger.info(f"Retrieved filepaths: {latest_files}.")
    return latest_files


def gcs_get_item_urls_from_filepaths(filepaths: List[str]) -> List[str]:
    """Returns content from item-url filepaths given as argument"""
    storage_client = storage.Client(GCP_PROJECT)
    bucket = storage_client.get_bucket(GCS_STORAGE)

    merged_urls = set()
    for filepath in filepaths:
        blob = bucket.get_blob(filepath)
        blob_text = blob.download_as_string().decode("utf-8")
        merged_urls.update(set(blob_text.split("\n")))
    logger.info(f"Retrieved {len(merged_urls)}.")
    return list(merged_urls)


def gcs_item_url_img_exists(item_url: str) -> bool:
    """Checks if images are scraped for given item_url"""
    storage_client = storage.Client(GCP_PROJECT)
    bucket = storage_client.bucket(GCS_STORAGE)
    img_filepath = item_url_to_img_path(item_url, numbered=True)
    return storage.Blob(bucket=bucket, name=img_filepath).exists(storage_client)


def gcs_file_exists(file: str) -> bool:
    """Checks if images are scraped for given item_url"""
    storage_client = storage.Client(GCP_PROJECT)
    bucket = storage_client.bucket(GCS_STORAGE)
    return storage.Blob(bucket=bucket, name=file).exists(storage_client)


def gcs_read_item_ids_from_jsonl(file: str) -> Set[str]:
    """Reads all the item ids from jsonl file from GCS"""
    assert file.endswith(".jsonl")
    if gcs_file_exists(file):
        storage_client = storage.Client(GCP_PROJECT)
        bucket = storage_client.get_bucket(GCS_STORAGE)
        blob = bucket.get_blob(file)
        blob_text = blob.download_as_string().decode("utf-8")
        return {json.loads(line)["id"] for line in blob_text.split("\n")}
    return set()


# Amazon S3 operations
def s3_upload_file(source_file_name: str, destination_file_name: str, bucket: str = S3_BUCKET):
    """Uploads a file to the S3 bucket"""
    s3 = boto3.client('s3')
    s3.upload_file(source_file_name, bucket, destination_file_name)
    logger.info(f"File {source_file_name} uploaded to {destination_file_name}.")


def s3_get_categories_from_item_url_files(site: str) -> List[str]:
    """Returns list of categories (e.g. ['muzi', 'zeny'] extracted from directory and file structure in S3"""
    s3 = boto3.client('s3')
    prefix = f"data/item_urls/{site}/"
    url_files = [obj['Key'].replace(prefix, '') for obj in s3.list_objects(Bucket=S3_BUCKET, Prefix=prefix)['Contents']]
    categories = list(set(map(lambda x: x.split("/")[0], url_files)))
    return categories


def s3_get_all_item_url_filepaths(site: str, categories: List[str]) -> List[str]:
    """Returns all item_url filepaths for given site and category"""
    logger.info(f"Retrieving all item_url files for {site} {categories}.")
    assert isinstance(categories, list), f"Categories are expected to be a list and not a {type(categories)}."
    s3 = boto3.client('s3')
    all_file_names = []
    for category in categories:
        prefix = f"data/item_urls/{site}/{category}/"
        all_file_names.extend([obj['Key'] for obj in s3.list_objects(Bucket=S3_BUCKET, Prefix=prefix)['Contents']])
    logger.info(f"Retrieved filepaths: {all_file_names}.")
    return all_file_names


def s3_get_last_item_url_filepath(site: str, categories: List[str]) -> List[str]:
    """Returns last item_url filepath for given site and category"""
    logger.info(f"Retrieving last item_url files for {site} {categories}.")
    latest_files = []
    for category in categories:
        prefix = f"data/item_urls/{site}/{category}/"
        file_names = s3_get_all_item_url_filepaths(site, [category])
        latest_datetime = sorted([i.split("/")[-1].split("_")[2].rstrip(".txt") for i in file_names])[-1]
        latest_files.append(os.path.join(prefix, f"item_urls_{latest_datetime}.txt"))
    logger.info(f"Retrieved filepaths: {latest_files}.")
    return latest_files


def s3_get_item_urls_from_filepaths(filepaths: List[str]) -> List[str]:
    """Returns content from item-url filepaths given as argument"""
    s3 = boto3.resource('s3')
    merged_urls = set()
    for filepath in filepaths:
        obj = s3.Object(S3_BUCKET, filepath)
        obj = obj.get()['Body'].read().decode('utf-8')
        merged_urls.update(set(obj.split("\n")))
    logger.info(f"Retrieved {len(merged_urls)}.")
    return list(merged_urls)


def s3_item_url_img_exists(item_url: str) -> bool:
    """Checks if images are scraped for given item_url"""
    s3_client = boto3.client('s3')
    img_filepath = item_url_to_img_path(item_url, numbered=True)
    objects = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=img_filepath, MaxKeys=1)
    return 'Contents' in objects


def s3_file_exists(file: str) -> bool:
    """Checks if images are scraped for given item_url"""
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=file, MaxKeys=1)
    return 'Contents' in objects


def s3_read_item_ids_from_jsonl(file: str) -> Set[str]:
    """Reads all the item ids from jsonl file from S3"""
    assert file.endswith(".jsonl")
    if s3_file_exists(file):
        s3 = boto3.resource('s3')
        obj = s3.Object(S3_BUCKET, file)
        obj = obj.get()['Body'].read().decode('utf-8')
        return {json.loads(line)["id"] for line in obj.split("\n")}
    return set()
