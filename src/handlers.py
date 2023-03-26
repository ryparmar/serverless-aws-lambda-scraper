import datetime
import logging
import os
import pathlib
import shutil

from src.config import SITE_NAME
from src.scrape_item_urls import scrape_item_urls
from src.scraping import (close_cookies_and_country_button,
                          setup_driver_for_docker)
from src.utils import remove_duplicate_urls_from_file, s3_upload_file

logger = logging.getLogger(__name__)


def setup_driver():
    driver = setup_driver_for_docker()
    close_cookies_and_country_button(driver)
    return driver


class Args:
    # Setup dummy args
    def __init__(self):
        self.categories = ["zeny", "muzi"]
        self.max_pages = 3
        self.output_dir = "/tmp/data"
        self.output_file = f"item_urls_{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')}.txt"
        self.save_to_gcs = False
        self.save_to_s3 = True
        self.clean_local_data = True
        self.debug = True


def handler_index(event=None, context=None) -> str:
    print("Scrapers are ready.")
    return "Scrapers are ready."


def handler_status(event=None, context=None) -> str:
    driver = setup_driver()
    return f"{driver.title} - {driver.current_url} - {driver.session_id}"


def handler_scrape_item_urls(event=None, context=None) -> str:
    driver = setup_driver()
    args = Args()

    # Setup logger
    log_filepath = f"/tmp/logs/{SITE_NAME}/{args.output_file.replace('.txt', '.log')}"
    pathlib.Path(f"/tmp/logs/{SITE_NAME}").mkdir(parents=True, exist_ok=True)

    # https://stackoverflow.com/questions/37703609/using-python-logging-with-aws-lambda
    if len(logging.getLogger().handlers) > 0:
        # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
        # `.basicConfig` does not execute. Thus we set the level directly.
        logging.getLogger().setLevel(logging.INFO)
        logging.getLogger().addHandler(logging.StreamHandler())
        logging.getLogger().addHandler(logging.FileHandler(log_filepath))
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=[
                logging.FileHandler(log_filepath),
                logging.StreamHandler(),
            ],
        )

    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Started at {start_time}")
    if args.debug:
        logger.info("Running in a debug mode")
    args_str = "\n".join(["\t{}: {}".format(k, v) for k, v in vars(args).items()])
    logger.info(f"Given arguments:\n{args_str}")

    scrape_item_urls(args, driver)

    # Deduplicate the urls
    logger.info("Removing duplicates from scraped item urls")
    for category in args.categories:
        url_filepath = os.path.join(args.output_dir, "item_urls", SITE_NAME, category, args.output_file)
        remove_duplicate_urls_from_file(url_filepath)

    # Save data to S3
    if args.save_to_s3:
        logger.info("Uploading log and item urls to S3")
        s3_upload_file(log_filepath, log_filepath.replace('/tmp/', ''))  # save log
        for category in args.categories:
            url_filepath = os.path.join(args.output_dir, "item_urls", SITE_NAME, category, args.output_file)
            s3_upload_file(url_filepath, url_filepath.replace('/tmp/', ''))

    # Remove local data
    if args.clean_local_data:
        logger.info("Cleaning local data - removing log and item scraped item url data")
        shutil.rmtree("/tmp/logs")
        shutil.rmtree(args.output_dir)
        logger.info("Local data removed")

    end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_time = {datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')}  # noqa
    result_message = (
        f"{SITE_NAME} item_urls for {args.categories} categories were scraped.",
        f"It took {run_time}.",
        f"Start {start_time} - end {end_time}."
    )
    logger.info(result_message)
    return result_message
