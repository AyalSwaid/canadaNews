from Collectors.NewsCollectors.NewsCollector import NewsCollector
from datetime import datetime
from Data.GLOBAL import Data
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import selenium.webdriver.support.expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import requests
import re
import csv
from dateutil.relativedelta import relativedelta
import random
import time
import os

import signal


class USA_NewsCollector(NewsCollector):
    def __init__(self, batch_size):
        super().__init__(batch_size)
        self.api_key = "n885BvJKUNu36DFFA5eSJLkthky7bG6S"
        self.batch_size = int(batch_size / 30)
        self.driver_path = Data.chrome_driver_path
        self.allowed_characters = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .,!?\'\";:()-")

        self.driver = None

    def init_driver(self):
        if self.driver is None:
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
            # driver = webdriver.Chrome(options=options,
            #                           executable_path=r"C:\Users\Jiana\Downloads\chromedriver-win64\chromedriver-win64"
            #                                           r"\chromedriver.exe")
            service = Service(executable_path=Data.chrome_driver_path)
            driver = webdriver.Chrome(options=options, service=service)
            self.driver = driver
            return driver
        return self.driver


    def get_news(self):
        to_write = []
        json_prog = Data.get_progress()

        date = datetime.strptime(json_prog["USA_news"], "%Y/%m")

        year = date.year
        month = date.month

        for i in range(self.batch_size):
            # url = f"https://api.nytimes.com/svc/archive/v1/{year}/{month}.json?api-key=n885BvJKUNu36DFFA5eSJLkthky7bG6S"
            #
            # response = requests.get(url)
            data = ["https://www.nytimes.com/2023/02/01/opinion/worker-overtime-protection-pay.html",
                   "https://www.nytimes.com/2023/02/01/opinion/worker-overtime-protection-pay.html",
                   "https://www.nytimes.com/2023/02/01/opinion/last-boeing-747-queen-of-the-skies.html",
                   "https://www.nytimes.com/2023/02/01/opinion/last-boeing-747-queen-of-the-skies.html",
                   "https://www.nytimes.com/2023/02/01/arts/concert-ticket-fees-biden.html",
                   "https://www.nytimes.com/2023/02/01/arts/concert-ticket-fees-biden.html",
                   "https://www.nytimes.com/2023/02/01/movies/kerry-condon-banshees-of-inisherin.html",
                   "https://www.nytimes.com/2023/02/01/movies/kerry-condon-banshees-of-inisherin.html",
                   "https://www.nytimes.com/2023/02/01/us/al-sharpton-tyre-nichols-funeral.html"]

            if data:
                for index, new in enumerate(data):
                    print(f"Process {index} from {len(data)}")
                    title = new["abstract"]
                    web_url = new["web_url"]
                    content = self.get_body(web_url)
                    if content:
                        pub_date = new["pub_date"]
                        date_part = pub_date.split('T')[0]

                        # Converting to datetime object
                        datetime_obj = datetime.strptime(date_part, "%Y-%m-%d")

                        # Formatting the date
                        pub_date = datetime_obj.strftime("%y-%m-%d")

                        to_write.append([title, pub_date, content, 1])
                    else:
                        print(f"content is empty ,url is {web_url}")
                        continue
                    # random_sleep_time = random.uniform(5, 30)
                    # time.sleep(random_sleep_time)
                    if index % 100 == 0 and index != 0:
                        x = str(datetime.now()).replace(':', "-")
                        csv_file_path = f"{Data.csv_files_dir}/news/USA/{x}.{month}.{year}.csv"
                        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                            writer = csv.DictWriter(csvfile, fieldnames=["title", "date", "content", "country"])
                            writer.writeheader()

                            writer = csv.writer(csvfile)
                            writer.writerows(to_write)
                            print("CSV file has been created successfully.")
                            to_write = []

                if to_write:
                    csv_file_path = f"{Data.csv_files_dir}/news/USA/{month}.{year}.csv"
                    with open(csv_file_path, 'w', newline='') as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=["title", "date", "content", "country"])
                        writer.writeheader()

                        writer = csv.writer(csvfile)
                        writer.writerows(to_write)
                        print("CSV file has been created successfully.")
                        to_write = []

                date = date + relativedelta(months=1)
                year = date.year
                month = date.month

                os.system("taskkill /F /IM chrome.exe")
                self.driver = None
        else:
            print("No data found")


        json_prog["USA_news"] = date
        Data.update_progress(json_prog)

    def contains_special_characters1(self, my_string):
        # Define a regular expression pattern to match any non-plain text characters
        # pattern = r'[^a-zA-Z0-9\s,.!?;:()\'\"]'
        # # Search for any matches
        # match = re.search(pattern, my_string)
        # # Return True if any matches are found, indicating the presence of special characters
        # return bool(match)



        # Iterate through each character in the string and check if it's in the allowed set
        for char in my_string:
            if char not in self.allowed_characters:
                return True
        return False

    def get_body(self, url):
        # options = Options()
        # options.add_argument('--no-sandbox')
        # options.headless = False
        # # options.add_experimental_option("prefs", prefs)
        # chrome_prefs = {
        #     "profile.default_content_setting_values": {
        #         "images": 2,
        #         "javascript": 2,
        #     }
        # }
        # options.experimental_options["prefs"] = chrome_prefs
        # # driver = webdriver.Chrome(options=options,
        # #                           executable_path=r"C:\Users\Jiana\Downloads\chromedriver-win64\chromedriver-win64"
        # #                                           r"\chromedriver.exe")
        # service = Service(executable_path=Data.chrome_driver_path)
        # driver = webdriver.Chrome(options=options, service=service)
        driver = self.init_driver()
        since = time.time()
        driver.get(url)
        print(f"elapsed: {time.time()-since}")
        # elements = driver.find_elements("xpath","//*")
        body = ""

        time.sleep(1)
        # try:
        #
        #     elements = WebDriverWait(driver, 3).until( \
        #         EC.visibility_of_all_elements_located((By.TAG_NAME, 'script')))
        # except:
        #     pass

        timeout = False
        while True:
            elements = driver.find_elements(By.TAG_NAME, "script")
            # Print the HTML source code of each element
            l = []


            for i in range(len(elements)-1, -1, -1):
                element = elements[i]
                try:
                    x = element.get_attribute('innerHTML')
                    if "window.__preloadedData" in x:
                        l.append(x)
                        break
                except Exception as e:
                    continue

            if l:
                your_string = l[-1]
                break
            else:
                if not timeout:
                    time.sleep(1)
                    timeout = True
                    continue
                print(f"bad url {url}")
                driver.quit()
                return body

        # Substring to remove
        substring_to_remove = '<script>window.__preloadedData = '
        body = ""
        # Remove the substring
        modified_string = your_string.replace(substring_to_remove, '')

        if not self.contains_special_characters1(modified_string):
            return modified_string

        pattern = r'{"__typename":"TextInline",.*?,"text":"'

        # Find all matches in the given string
        matches = re.finditer(pattern, modified_string, re.DOTALL)

        # Print the extracted texts
        # print(len(matches))
        ct = 0
        for text in matches:
            ct += 1
            start_index = text.end()
            end_index = modified_string.find('"', start_index)

            # Ensure that the end index was found
            if end_index != -1:
                # Extract the content between the start and end indices
                content = modified_string[start_index:end_index]

                # Print the extracted content

                # Accumulate the content
                body += content
            else:
                print(f"no contentt found : {url}")
        # os.system("taskkill /F /IM chrome.exe")
        print(ct)
        return body


import json


def contains_special_characters(my_string):
    # Define a regular expression pattern to match any non-plain text characters
    pattern = r'[^a-zA-Z0-9\s,.!?;:()\'\"]'
    # Search for any matches
    match = re.search(pattern, my_string)
    # Return True if any matches are found, indicating the presence of special characters
    return bool(match)


def get_body7(url, ):
    options = Options()
    options.add_argument('--no-sandbox')
    options.headless = False
    driver = webdriver.Chrome(options=options,
                              executable_path=r"C:\Users\Jiana\Downloads\chromedriver-win64\chromedriver-win64"
                                              r"\chromedriver.exe")

    driver.get(url)
    elements = driver.find_elements_by_xpath("//*")
    body = ""
    # print(elements)

    # Print the HTML source code of each element
    l = []
    for element in elements:
        try:
            x = element.get_attribute("outerHTML")
            if "<script>window.__preloadedData" in x:
                l.append(x)
        except Exception as e:
            continue

    print("hi")
    if l:
        your_string = l[-1]
    else:
        print(f"bad url {url}")
        driver.quit()
        return body

    # Substring to remove
    substring_to_remove = '<script>window.__preloadedData = '
    body = ""
    # Remove the substring
    modified_string = your_string.replace(substring_to_remove, '')
    print(modified_string)
    print(";;;")
    print(contains_special_characters(modified_string))

    pattern = r'{"__typename":"TextInline",.*?,"text":"'

    # Find all matches in the given string
    matches = re.finditer(pattern, modified_string, re.DOTALL)
    if not matches:
        if modified_string:
            return modified_string
        else:
            return body

    # Print the extracted texts
    for text in matches:
        # print(text)
        # end_index = modified_string.find('"', text.start()+text.end())
        # # Extract the content between the indices
        # content = modified_string[text.start() + text.end():end_index + 1]
        #
        # body += content
        start_index = text.end()
        end_index = modified_string.find('"', start_index)

        # Ensure that the end index was found
        if end_index != -1:
            # Extract the content between the start and end indices
            content = modified_string[start_index:end_index]

            # Print the extracted content

            # Accumulate the content
            body += content
        else:
            print(f"no content found : {url}")
    os.system("taskkill /F /IM chrome.exe")
    return body


# print(get_body7("https://www.nytimes.com/2022/12/31/sports/ncaafootball/fiesta-bowl-michigan-tcu.html"))
# print(get_body7("https://www.nytimes.com/2022/12/31/sports/ncaafootball/fiesta-bowl-michigan-tcu.html"))
# lst = ["https://www.nytimes.com/2023/02/01/opinion/worker-overtime-protection-pay.html","https://www.nytimes.com/2023/02/01/opinion/worker-overtime-protection-pay.html","https://www.nytimes.com/2023/02/01/opinion/last-boeing-747-queen-of-the-skies.html","https://www.nytimes.com/2023/02/01/opinion/last-boeing-747-queen-of-the-skies.html","https://www.nytimes.com/2023/02/01/arts/concert-ticket-fees-biden.html","https://www.nytimes.com/2023/02/01/arts/concert-ticket-fees-biden.html","https://www.nytimes.com/2023/02/01/movies/kerry-condon-banshees-of-inisherin.html","https://www.nytimes.com/2023/02/01/movies/kerry-condon-banshees-of-inisherin.html","https://www.nytimes.com/2023/02/01/us/al-sharpton-tyre-nichols-funeral.html"]

x = USA_NewsCollector(50)
since = time.time()
# print(x.allowed_characters)
print((x.get_news()))
# print(len(x.get_body("https://www.nytimes.com/2023/02/02/world/australia/nick-kyrgios-assault.html")))
print("elapsed:", time.time() - since)