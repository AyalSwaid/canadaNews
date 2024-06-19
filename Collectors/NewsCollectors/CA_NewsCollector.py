from Collectors.NewsCollectors.NewsCollector import NewsCollector
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import requests
import re
from bs4 import BeautifulSoup
from Data.GLOBAL import Data
import time
import random
from datetime import datetime, timedelta
import csv
import os
from selenium.webdriver.chrome.service import Service



class CA_NewsCollector(NewsCollector):
    def __init__(self, batch_size):
        super().__init__(batch_size)
        # self.driver_path = r"C:\Users\Jiana\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"
        self.driver_path = Data.chrome_driver_path
        self.driver = None

    def get_news(self):
        #TODO quit the driver
        to_write = []
        json_prog = Data.get_progress()
        date_str = datetime.strptime(json_prog["CA_news"], "%Y/%m/%d")

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36'
        }

        for i in range(self.batch_size):
            formatted_date = date_str.strftime("%Y-%m-%d")
            url = f"https://nationalpost.com/sitemap/{formatted_date}/"
            print("curr_url:", url)
            # Send a GET request to the page with headers
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                # Find all <a> tags with the desired pattern
                links = soup.find_all('a', href=True)
                patterns = ['/pmn/news-pmn/', '/news/canada/', '/news/world/']

                # Filter links based on the patterns in the href attribute
                filtered_links = [link for link in links if
                                  any(link['href'].startswith(pattern) for pattern in patterns)]


                # Print the filtered links
                for m, link in enumerate(filtered_links):
                    print(f"process {m} from {len(filtered_links)}")
                    content = ""
                    title = link.get_text(strip=True)
                    driver = self.init_driver()
                    driver.get("https://nationalpost.com" + link["href"])
                    try:
                        write_date = driver.find_element(By.CLASS_NAME, "published-date__since")
                        write_date = write_date.text
                        date_pattern = r'Published (\w+ \d{1,2}, \d{4})'
                        match = re.search(date_pattern, write_date)

                        if match:
                            date_string = match.group(1)
                            date_variable = datetime.strptime(date_string, '%b %d, %Y')
                            write_date = date_variable.strftime('%Y-%m-%d')
                        else:
                            print("No date found in the text.")

                    except Exception as e:
                        write_date = formatted_date

                    try:
                        parent_elements = driver.find_elements(By.CLASS_NAME, 'article-content__content-group--story')
                    except Exception as e:
                        continue
                    # Extract all <p> tags within the parent element
                    stop = False
                    for parent_element in parent_elements:
                        p_tags = parent_element.find_elements(By.TAG_NAME, 'p')
                        for p in p_tags:
                            if p.text == "___":
                                stop = True
                                break
                            else:
                                content += p.text
                        if stop:
                            break

                    to_write.append([title, write_date, content, 4])
                    # random_sleep_time = random.uniform(0.5, 1.5)
                    # time.sleep(random_sleep_time)

                date_str = date_str + timedelta(days=1)
                if i % 100 == 0 and i != 0:
                    self.quit_driver()
                    csv_file_path = f"{Data.csv_files_dir}/news/CA/{str(datetime.now()).replace(':', '-')}.csv"
                    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=["title", "date", "content", "country"])
                        writer.writeheader()

                        writer = csv.writer(csvfile)
                        writer.writerows(to_write)
                        to_write = []
                        print("CSV created")
            else:
                print(f'Failed to retrieve the page. Status code: {response.status_code}')

        if to_write:
            csv_file_path = f"{Data.csv_files_dir}/news/CA/{str(datetime.now()).replace(':', '-')}.csv"
            with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=["title", "date", "content", "country"])
                writer.writeheader()

                writer = csv.writer(csvfile)
                writer.writerows(to_write)
                print("CSV created")

        self.quit_driver()
        last_date = date_str
        json_prog["CA_news"] = last_date.strftime("%Y/%m/%d")
        Data.update_progress(json_prog)

    def quit_driver(self):
        if self.driver:
            self.driver.quit()
            os.system("taskkill /F /IM chrome.exe")
            self.driver = None


    def init_driver(self):
        if self.driver is None:
            # options = uc.ChromeOptions()
            service = Service(executable_path=Data.chrome_driver_path)

            options = Options()
            options.add_argument('--no-sandbox')
            options.headless = False
            # options.add_experimental_option("prefs", prefs)
            chrome_prefs = {
                "profile.default_content_setting_values": {
                    "images": 2,
                    "javascript": 2,
                }
            }
            options.experimental_options["prefs"] = chrome_prefs
            # driver = uc.Chrome(options=options, version_main=126,
            #                    driver_executable_path=r"C:\Users\Jiana\Downloads\chromedriver-win64\chromedriver-win64"
            #                                           r"\chromedriver.exe")
            driver = webdriver.Chrome(options=options, service=service)

            self.driver = driver
        return self.driver
