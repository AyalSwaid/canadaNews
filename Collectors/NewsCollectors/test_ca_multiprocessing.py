import pandas as pd

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

import multiprocessing as mp



class CA_NewsCollector(NewsCollector):
    def __init__(self, batch_size):
        super().__init__(batch_size)
        # self.driver_path = r"C:\Users\Jiana\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"
        self.driver_path = Data.chrome_driver_path
        self.driver = None

        self.n_workers = 4
        self.to_write = [[] for _ in range(self.n_workers)]
        self.drivers = [None for _ in range(self.n_workers)]

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
            print(self.drivers)
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
                filtered_links = [(link.get_text(), link['href']) for link in links if
                                  any(link['href'].startswith(pattern) for pattern in patterns)]


                # step_size = len(filtered_links) // self.n_workers
                # self.splits = tuple(tuple(sl) for sl in self.split_list(filtered_links, self.n_workers))
                self.splits = self.split_list(filtered_links, self.n_workers)
                workers = []
                print("starting processes")
                # start 4 processes to process the list
                # res_queue = mp.Queue()
                for worker_idx in range(self.n_workers):
                    print(worker_idx)

                    # TODO: fix driver quit
                    process = mp.Process(target=self.processing_worker, args=(worker_idx, formatted_date))
                    workers.append(process)
                    process.start()
                    time.sleep(1) # for driver load

                for worker in workers:
                    worker.join()

                print("all processes finished")

                # close drivers
                os.system("taskkill /F /IM chrome.exe")

                # collect results from workers
                for w_idx in range(self.n_workers):
                    to_write.extend(pd.read_csv(f"{w_idx}.csv").values.tolist())

                # [to_write.extend(worker_res) for worker_res in self.to_write]
                # self.to_write = [[] for _ in range(self.n_workers)]
                # results = []
                # while not res_queue.empty():
                #     to_write.append(res_queue.get())
                # print("to_write", to_write)
                date_str = date_str + timedelta(days=1)
                if i % 100 == 0 and i != 0:
                    self.quit_drivers()
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

        self.quit_drivers()
        last_date = date_str
        json_prog["CA_news"] = last_date.strftime("%Y/%m/%d")
        Data.update_progress(json_prog)

    def split_list(self, lst, n):
        # Calculate the size of each split
        k, m = divmod(len(lst), n)
        splits = [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]
        return splits

    def quit_drivers(self):
        for d in range(len(self.drivers)):
            if self.drivers[d]:
                self.drivers[d].quit()
                os.system("taskkill /F /IM chrome.exe")
                self.drivers[d] = None


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


    def init_worker_driver(self, worker_idx):
        if self.drivers[worker_idx] is None:
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

            self.drivers[worker_idx] = driver
        return self.drivers[worker_idx]

    def processing_worker(self, worker_idx, formatted_date):
        filtered_links = self.splits[worker_idx]
        # filtered_links = tuple()
        # print(worker_idx)
        to_write = []
        driver = self.init_worker_driver(worker_idx)
        # Print the filtered links
        for m, link in enumerate(filtered_links):
            print(f"process {m} from {len(filtered_links)}")
            content = ""
            # title = link.get_text(strip=True)
            title = link[0]
            # driver = self.init_worker_driver(worker_idx)
            driver.get("https://nationalpost.com" + link[1])
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
                        # stop = True
                        # break
                if stop:
                    break
            # print(content)
            to_write.append([title, write_date, content, 4])
            # self.to_write[worker_idx].append([title, write_date, content, 4])
            # res_queue.put([title, write_date, content, 4])
            # random_sleep_time = random.uniform(0.5, 1.5)
            # time.sleep(random_sleep_time)

        # with open(f"{worker_idx}.txt", "w") as f:
        #     f.writelines(to_write)

        pd.DataFrame(to_write).to_csv(f"{worker_idx}.csv")

    def collect_workers_results(self):
        res = []
        for i in range(self.n_workers):
            res.extend(pd.read_csv(f"{i}.csv").values.tolist())

        return res


# print(pd.read_csv(r"C:\Users\ayals\OneDrive\שולחן העבודה\parliamentMining\Data\csv_files\news\CA\2024-06-19 21-18-13.935680.csv").shape)