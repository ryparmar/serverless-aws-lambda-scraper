import logging
import time
from random import randint
from typing import List, Optional, Set

from fake_useragent import UserAgent
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from selenium import webdriver
from src.config import BINARY_PATH, DRIVER_PATH, HOME_URL, WAIT

# Setup module logger
logger = logging.getLogger(__name__)


def setup_driver() -> webdriver:
    """Setup Selenium Chrome driver open HOME_URL defined in config and reject all cookies"""
    options = webdriver.ChromeOptions()
    options.binary_location = BINARY_PATH
    service = Service(DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    driver.implicitly_wait(5)
    return driver


def setup_driver_options_for_docker() -> webdriver.ChromeOptions:
    """Setup driver options for headless Chrome in Docker container"""
    chrome_options = webdriver.ChromeOptions()
    chrome_options.binary_location = BINARY_PATH
    chrome_options.add_argument("--headless")  # Hide the GUI
    chrome_options.add_argument("--no-sandbox")  # No protection needed
    chrome_options.add_argument("--window-size=1280x1696")  # Setup a fixed screens size
    chrome_options.add_argument("--single-process")  # Lambda only give us only one CPU
    chrome_options.add_argument("--no-zygote")  # Don't create zygote processes because Lambda give us only one CPU
    chrome_options.add_argument("--disable-dev-shm-usage")  # Create temporary folder for shared memory files
    chrome_options.add_argument("--disable-dev-tools")  # Disable Chrome dev tools

    ua = UserAgent(browsers=["chrome"])
    user_agent = ua.random
    chrome_options.add_argument(f"user-agent={user_agent}")
    return chrome_options


def setup_driver_for_docker() -> webdriver:
    """Setup Selenium Chrome driver for headless Chrome in Docker container"""
    chrome_options = setup_driver_options_for_docker()
    service = Service(DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.implicitly_wait(5)
    return driver


def close_cookies_and_country_button(driver: webdriver) -> None:
    """Go to home url and reject all cookies"""
    driver.get(HOME_URL)
    if country_button_present(driver):
        click_to_country_button(driver)
    time.sleep(randint(3, 5))
    if cookies_button_present(driver):
        click_to_reject_all_cookies_button(driver)


def get_item_urls_from_page(driver: webdriver) -> Set[str]:
    """Return all urls of items from a page"""
    return_urls = set()
    try:
        WebDriverWait(driver, WAIT).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[class=feed-grid__item] a[href]"))
        )
        # There are supposed to be an even number of links.
        # Each item needs to have 2 links (1 for seller and 1 for item)
        links = [
            link.get_attribute("href")
            for link in driver.find_elements(by=By.CSS_SELECTOR, value="div[class=feed-grid__item] a[href]")
        ]
        num_of_links = len(links)
        logger.info(f"Found {num_of_links} links")
        if num_of_links % 2 == 0:
            return_urls = set(map(str, links[1::2]))
    except Exception as E:
        logger.warning(f"Items were not returned! {E}")
    finally:
        return return_urls


def get_next_page_button(driver: webdriver) -> Optional[WebElement]:
    """Return clickable WebElement if the current page is not the last one"""
    button_element = None
    try:
        WebDriverWait(driver, WAIT).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[class*='Pagination__next']")))
        button_element = driver.find_element(By.CSS_SELECTOR, "a[class*='Pagination__next']")
    except Exception as E:
        logger.warning(f"Button was not found! {E}")
    finally:
        return button_element


def cookies_button_present(driver: webdriver) -> bool:
    """Check whether the cookies button is present on the website"""
    if driver.find_elements(By.CSS_SELECTOR, "[id^='onetrust-reject-all-handler']"):
        return True
    return False


def click_to_reject_all_cookies_button(driver: webdriver) -> None:
    """Click on the reject all cookies button, so the next page button is clickable"""
    try:
        WebDriverWait(driver, WAIT).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "[id^='onetrust-reject-all-handler']"))
        )
        driver.find_element(By.CSS_SELECTOR, "[id^='onetrust-reject-all-handler']").click()
    except Exception as E:
        logger.warning(f"Button to reject all cookies was not found! {E}")


def country_button_present(driver: webdriver) -> bool:
    """Check whether the country button is present on the website"""
    if driver.find_elements(
        By.CSS_SELECTOR, "[class*='web_ui__Cell__cell web_ui__Cell__default web_ui__Cell__clickable']"
    ):
        return True
    return False


def click_to_country_button(driver: webdriver, country: str = "Česká republika") -> None:
    """Click on the Czech location button, so the next page button is clickable"""
    try:
        countries = driver.find_elements(
            By.CSS_SELECTOR, "[class*='web_ui__Cell__cell web_ui__Cell__default web_ui__Cell__clickable']"
        )
        for i, country_choice in enumerate(countries):
            if country_choice.text == country:
                countries[i].click()
                return
        raise ValueError(f"Your country {country} was not a choice in found countries.")
    except Exception as E:
        logger.warning(f"Button to choose country was not found! {E}")


def scrape_element(driver: webdriver, css_selector: By.CSS_SELECTOR) -> str:
    """Scrape a single element given by css selector that will be converted to text"""
    el_value = ""
    try:
        WebDriverWait(driver, WAIT).until(EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
        el_value = driver.find_element(By.CSS_SELECTOR, css_selector).text
    except Exception as E:
        logger.warning(f"Element {css_selector} was not scraped! {E}")
    finally:
        return el_value


def scrape_elements(driver: webdriver, css_selector: By.CSS_SELECTOR) -> List[str]:
    """Scrape elements given by css selector that will be converted to text"""
    el_values = []
    try:
        WebDriverWait(driver, WAIT).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, css_selector)))
        el_values = [i.text for i in driver.find_elements(By.CSS_SELECTOR, css_selector)]
    except Exception as E:
        logger.warning(f"Elements {css_selector} was not scraped! {E}")
    finally:
        return el_values


def scrape_image_urls(driver: webdriver, css_selector: By.CSS_SELECTOR) -> None:
    """Scrape images urls given by css selector"""
    img_urls = []
    try:
        WebDriverWait(driver, WAIT).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, css_selector)))
        img_urls = [i.get_attribute("href") for i in driver.find_elements(By.CSS_SELECTOR, css_selector)]
    except Exception as E:
        logger.warning(f"Elements {css_selector} was not scraped! {E}")
    finally:
        return img_urls
