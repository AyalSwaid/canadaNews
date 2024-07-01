import re

import pandas as pd

from Collectors.DataCollectors.DataCollector import DataCollector
# from DataCollector import DataCollector
import time
import selenium.webdriver.support.expected_conditions as EC
import undetected_chromedriver as uc
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import os
import pickle
from Data.GLOBAL import Data
# import ParliamentMining.DataPipeline
# import Data
from datetime import datetime, timedelta
from random import random
import requests as reqs


class UK_DataCollector(DataCollector):
    def __init__(self, batch_size, txt_files_dir = "text_files"):
        super(UK_DataCollector, self).__init__(batch_size)

        # must give full path for download_dir !!!
        self.download_dir = r"C:\Users\ayals\OneDrive\שולחן העבודה\parliamentMining\Collectors\DataCollectors\test_downloads"
        # self.txt_files_dir = txt_files_dir
        self.txt_files_dir = Data.text_files_dir
        self.processor_files = Data.processor_dir
        self.batch_size = batch_size

        self.failed_links = []
        self.members = pd.read_csv("Data/csv_files/members/UK_members.csv")

        self.member_id2name_cache = dict(self.members[["og_id", "name"]].values)
        self.members_set = set(self.members["name"].values)
        print("finish init")

    def get_debates(self):
        print("collecting UK debates")
        driver = self.init_driver()
        self.failed_links = []

        # get dates range
        json_prog = Data.get_progress()
        start_date = json_prog['UK_debates_start_date']
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = start_date + timedelta(days=self.batch_size)

        links = self.get_debates_links(driver, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), 1)
        print(f"Collector (UK debates) recognized {len(links)} links.")
        all_debates = []
        empty_speeches = []

        for idx, link in enumerate(links):
            debate_metadata, speeches, members = self.extract_speeches_from_link(driver, link)

            if not speeches:
                empty_speeches.append(link)
                continue

            # make json file
            # save json file path in debate_metadata
            speeches_file_path = f"{Data.speeches_files_dir}/UK/{idx}mW{str(datetime.now()).replace(':', '-')}.json"
            debate_metadata["file_path"] = speeches_file_path
            debate_metadata["members"] = members
            Data.save_json(speeches_file_path, speeches)

            # save all debates as csv
            all_debates.append(debate_metadata)

        pd.DataFrame(all_debates).to_csv(f"{Data.csv_files_dir}/debates/a{str(datetime.now()).replace(':', '-')}.csv", index=False)
        json_prog['UK_debates_start_date'] = end_date.strftime("%Y-%m-%d")


        Data.save_json(f"{Data.csv_files_dir}/debates/empty_links{str(datetime.now()).replace(':', '-')}.json", empty_speeches)


        Data.update_progress(json_prog)

        time.sleep(2)
        driver.quit()
        os.system("taskkill /F /IM chrome.exe")
        print("DONE UK debates")
    def init_driver(self):
        '''
        init chrome webdriver object and set options to start scraping.
        also creates the downloades & text files folders
        :return: webdriver object
        '''

        options = uc.ChromeOptions()

        options.add_argument("--start-maximized")
        options.add_argument('--blink-settings=imagesEnabled=false')
        download_dir = self.txt_files_dir + "/UK/"
        download_dir = os.path.join(os.getcwd(), download_dir.replace("/", "\\"))

        options.add_experimental_option("prefs", {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,  # Disable prompting for download
            "download.directory_upgrade": True})

        # create needed folders for downloads
        for dir_path in [self.download_dir, self.txt_files_dir]:
            try:
                os.mkdir(dir_path)
                print(f"Directory '{dir_path}' created successfully.")
            except FileExistsError:
                pass

        # driver = uc.Chrome(version_main=120, options=options)
        driver = uc.Chrome(options=options, version_main=120, driver_executable_path=Data.chrome_driver_path)
        return driver

    def extract_speeches_from_link(self, driver, link):
        # url_endpoint = "https://hansard.parliament.uk"
        country = Data.country2code["uk"]
        speeches = []
        metadata = {}
        members = set()

        url = link
        print("url:", url)
        driver.get(url)

        # date,debate_title,country,file_path,members
        # get date
        metadata["date"] = link.split("/")[4]
        try:
            metadata["debate_title"] = driver.find_element(By.XPATH, "/html/body/main/div[1]/div[1]/div/div[1]/h1").text
        except:
            return metadata, speeches, members

        metadata["country"] = country
        # speeches
        debate_tags = driver.find_element(By.XPATH, '//div[contains(@class, "primary-content")]')
        # print("here", debate_tags)


        inner = debate_tags.find_elements(By.XPATH, "./*")
        if len(inner) == 1 and inner[0].get_attribute("class") == "child-debate-list":
            speeches_iter = inner[0].find_elements(By.XPATH,"./*")
        else:
            speeches_iter = inner


        for child_tag in speeches_iter:
            # print(child_tag.get_attribute("class"))

            if child_tag.get_attribute("class") == "child-debate":
                tags = child_tag.find_elements(By.XPATH, "./*")
            else:
                tags = [child_tag]
            # print(len(tags))
            for tag in tags:
                if tag.get_attribute("class") == "debate-item debate-item-contributiondebateitem":
                    sub_elements = tag.find_element(By.XPATH, './*').find_elements(By.XPATH, './*')
                    header, content = sub_elements[0], sub_elements[1]

                    primary, secondary = None, None
                    try:
                        primary = header.find_element(By.CLASS_NAME, "primary-text").text.strip()
                    except NoSuchElementException:
                        # print("NoSuchElementException primary, its okay")

                        pass

                    # try:
                    #     secondary = header.find_element(By.CLASS_NAME, "secondary-text").text.strip().strip("(").strip(")")
                    # except NoSuchElementException:
                    #     # print("NoSuchElementException secondary, its okay")
                    #     pass



                    name_link = header.find_element(By.TAG_NAME, "a") # example of name link: /search/MemberContributions?house=Commons&memberId=4099
                    # name = name_link.text
                    try:
                        name_id = re.search(r"memberId=(\d+)", name_link.get_attribute("href")).group(1)
                    except AttributeError:
                        name_id = -1
                    # name_id = name_id if name_id is not None else -1

                    name_id = int(name_id)
                    if name_id == -1:
                        # name = self.search_in_members(primary, secondary)
                        name = primary
                    else:
                        name = self.member_id2name(name_id)
                        name = primary if name is None else name

                    speech = content.text

                    speeches.append({
                        "name": name,
                        "id": name_id,
                        "speech": speech
                    })
                    members.add(name)

        if not speeches:

            print(speeches)

        return metadata, speeches, members

    def search_in_members(self, primary, secondary):
        rep_name = re.compile(r'\s*\(?((?:MR\.?|MRS\.?|MS\.?|Dr\.?)\s+)?((?:\w|-|\s)+)\)?', re.IGNORECASE)

        for name in [primary, secondary]:
            # print(name)
            if name is None:
                continue

            for option in [name, rep_name.search(name).group(2)]:
                if option in self.members_set:
                # if (self.members["name"] == option).any():
                    return option


        return None

    def member_id2name(self, name_id):
        # if self.member_id2name_cache.get(name_id) is not None:
        #     return self.member_id2name_cache.get(name_id)
        return self.member_id2name_cache.get(name_id, None)
        # name = self.members[self.members["og_id"] == name_id]
        # # TODO: check if using driver
        #
        # if len(name) == 0:
        #     return None
        # else:
        #     self.member_id2name_cache[name_id] = name["name"].values[0]
        #     return name["name"].values[0]

        # pass

    def get_debates_links(self, driver: uc.Chrome, start_date, end_date, start_page=1, final_page=0):
        '''
        get all the links for debates happened between start date - end date
        driver: selenium WebDriver
        the function iterates on the website pages, each page is 20 debates
        start_date: string of format "yyyy-mm-dd", example: "2000-01-01"
        end_date: string of format "yyyy-mm-dd", example: "2000-01-01"
        return: list of strings, including the links
        '''
        url = f"https://hansard.parliament.uk/search/Debates?endDate={end_date}&partial=False&sortOrder=1&startDate={start_date}"

        driver.get(url + f'&page={start_page}')

        try:
            n_pages = WebDriverWait(driver, 5).until( \
                EC.visibility_of_element_located((By.XPATH, '/html/body/main/div[2]/article/div/div[2]/div[2]/div/div[1]/div/strong[3]'))).text

            n_pages = int(n_pages)
        except TimeoutException:
            print("no debates for this period")
            return []

        if final_page == 0:
            final_page = n_pages + 1
        else:
            final_page = min(final_page, n_pages + 1)

        links = []

        for page in range(start_page, final_page):
            driver.get(url + f'&page={page}')
            search_results = WebDriverWait(driver, 30).until( \
                EC.visibility_of_any_elements_located((By.XPATH, "/html/body/main/div[2]/article/div/div[2]/div[3]/a")))

            links.extend([result.get_attribute("href") for result in search_results])

            # if page != final_page:
        # del driver

        return links

if __name__ == "__main__":
    t = "https://hansard.parliament.uk/Commons/2018-02-08/debates/87DB5387-BF64-4D07-8BFF-93975485B7D8/SuperfastBroadbandNorthEastHertfordshire"
    # t = "https://hansard.parliament.uk/Commons/2020-02-27/debates/7E397263-07BF-4D8B-8ABB-F927ACA66759/BusinessOfTheHouse"
    x = UK_DataCollector(15)
    d = x.init_driver()
    print(x.extract_speeches_from_link(d, t))

    d.quit()
    os.system("taskkill /F /IM chrome.exe")
