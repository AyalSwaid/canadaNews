# from Collectors.DataCollectors.DataCollector import DataCollector
import re

import pandas as pd

from Collectors.DataCollectors.DataCollector import DataCollector
# from DataCollector import DataCollector
import time
import selenium.webdriver.support.expected_conditions as EC
import undetected_chromedriver as uc
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
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
        # debates = []
        self.__download_uk_txt_files(driver, links)
        debates = self.__get_uk_debates_metadata()
        # for (debate_file, debate_date) in self.get_debates_files(driver, links):
        #     debates.append({
        #         'date': debate_date,
        #         'file_path': debate_file
        #     })

        # save current batch data in pickle file for the processor
        if debates:
            pkl_file_name = str(datetime.now()).replace(':', "-")
            with open(f'{Data.processor_debates_dir}/UK/{pkl_file_name}.pkl', 'wb') as f:
                pickle.dump(debates, f)
        driver.quit()

        Data.update_failed_links(self.failed_links)


        print(f'Collected {len(links)}, Failed: {len(self.failed_links)}')

        json_prog['UK_debates_start_date'] = end_date.strftime("%Y-%m-%d")
        Data.update_progress(json_prog)

        time.sleep(5)
        driver.quit()
        print("DONE UK debates")




    def get_votes(self):
        pass


    def get_members(self):


        # if this flag is True then raad the pkl file that already scraped members ids to use them later with the members api
        pickle_og_ids = True

        # get all members uk ids
        if pickle_og_ids:
            og_ids = Data.load_pkl("og_members_ids")
        else:
            driver = self.init_driver()
            og_ids = self.__get_members_og_ids(driver)
            driver.quit()
            Data.save_pkl(og_ids, "og_members_ids")


        # get historical data of each member using api
        all_MPs = self.__get_historical(og_ids)

        # save all data in the scheme
        all_MPs = pd.DataFrame(all_MPs)



        # save MPs
        csv_file_name = "UK_members.csv"
        try:
            all_MPs.to_csv(f"{Data.csv_files_dir}/members/{csv_file_name}", index_label='member_id')
        except:
            print("error while saving csv, fixed by abo swaid")
            all_MPs.to_csv(f"{Data.csv_files_dir}/members/{csv_file_name}")

        # save parties
        # all_MPs = pd.read_csv(f"{Data.csv_files_dir}/members/uk_members.csv")
        # parties = list(set(all_MPs['party'].values))
        # parties_df = pd.DataFrame(parties)
        # parties_df.columns = ['party_name']
        # parties_df["country"] = Data.country2code['uk']
        #
        # csv_file_name = "UK_parties.csv"
        # parties_df.to_csv(f"{Data.csv_files_dir}/parties/{csv_file_name}")
        # res = reqs.get("https://members-api.parliament.uk/api/Members/History/172", headers={"accept": "text/plain"})
        #
        # print(res.content)


    def get_bills(self):
        sessions_range = {"start": 13, "end": 38+1} # 13 is since 2000 but it doesnt return results from the API
        sessions_range = {"start": 18, "end": 38+1}
        sessions_range = {"start": 23, "end": 24+1}

        all_bills = []
        print("Collector (UK bills) started")
        # iterate over bill ids (use skip 20*i)
        for session in range(sessions_range['start'], sessions_range['end']):
            print("session: ", session)
            # for each bill  get its id and title from the same result
            bills = self.__get_session_bills_ids(session)
            print(len(bills), "bills found")
            # continue


            # get bill introduction date
            # each (bill_id, title, date) is a single row
            bills = self.__get_bills_intro_date(bills)

            all_bills.extend(bills)


        all_bills_df = pd.DataFrame(all_bills)
        all_bills_df.to_csv(f"{Data.csv_files_dir}/bills/uk_bills.csv")







    def init_driver(self):
        '''
        init chrome webdriver object and set options to start scraping.
        also creates the downloades & text files folders
        :return: webdriver object
        '''


        options  = uc.ChromeOptions()

        options.add_argument("--start-maximized")
        options.add_argument('--blink-settings=imagesEnabled=false')
        download_dir = self.txt_files_dir + "/UK/"
        download_dir = os.path.join(os.getcwd(), download_dir.replace("/", "\\"))

        options.add_experimental_option("prefs", {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False, # Disable prompting for download
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


    def new_file_name(self):
        '''
        Checks if the file in download_dir is still downloading or finished.
        Note: the function assumes that there is only one file donwloading at a time.
        :return: None if file still didnt finish else return file name
        '''
        download_dir = self.download_dir

        filename = None
        for filename in os.listdir(download_dir):

            if filename.endswith(".tmp"):
                return None
        return filename

    def __download_uk_txt_files(self, driver, links):
        download_dir = self.download_dir
        txt_files_dir = self.txt_files_dir
        failed_links = []

        for link in links:
            link = link.split(r'/')
            # debate_date = link[-4]
            text_file_url = "https://hansard.parliament.uk/debates/GetDebateAsText/" + link[-2]

            # download file by url GET
            driver.get(text_file_url)

            if driver.title == 'An error has occurred - Hansard - UK Parliament':
                print(f"cant download debate: {'/'.join(link)}")
                self.failed_links.append('/'.join(link))
                driver.get('https://www.google.com/')
                continue

            # wait untill file is downloaded
            timeout = 0.5 + random()
            time.sleep(timeout)
            # debate_file = self.new_file_name()
            # while debate_file is None and timeout > 0:
            #     time.sleep(0.1)
            #     debate_file = self.new_file_name()
            #     timeout -= 0.1

            # if timeout <= 0:
            #     print(f"cant download debate: {'/'.join(link)}")
            #     self.failed_links.append('/'.join(link))
            #     continue

    def __get_uk_debates_metadata(self):
        debates = []

        rep_date = re.compile(r"\d+-\d+-\d+")
        for file_path in os.listdir(self.txt_files_dir + "/UK/"):
            try:
                debate_date = rep_date.findall(file_path)[-1] # did [-1] in case there was another date in the file name
            except:
                print("Processor(debate UK) couldnt extract debate date from txt file: ", file_path)
                continue
            debates.append({
                'debate_date': debate_date,
                'content_file_path': f"{self.txt_files_dir}/UK/{file_path}"
            })

        return debates


    def get_debates_files(self, driver, links):
        '''
        given the links of the debates in hansard website, get debate date and
        download its text file for each link in links list.
        the function moves the downloaded file from download_dir into text_files dir to get processed later
        :driver: webdriver object
        :links: list of string, containing debates links
        :return: generator:
                debate_file: txt file name
                deate date: string, presenting the date of the debate of format "yyyy-mm-dd"
        '''
        download_dir = self.download_dir
        txt_files_dir = self.txt_files_dir
        failed_links = []


        for link in links:
            link = link.split(r'/')
            debate_date = link[-4]
            text_file_url = "https://hansard.parliament.uk/debates/GetDebateAsText/" + link[-2]

            # download file by url GET
            driver.get(text_file_url)

            if driver.title == 'An error has occurred - Hansard - UK Parliament':
                print(f"cant download debate: {'/'.join(link)}")
                self.failed_links.append('/'.join(link))
                driver.get('https://www.google.com/')
                continue

            # wait untill file is downloaded
            timeout = 3.5
            time.sleep(timeout)
            debate_file = self.new_file_name()
            while debate_file is None and timeout > 0:
                time.sleep(0.1)
                debate_file = self.new_file_name()
                timeout -= 0.1

            if timeout <= 0:
                print(f"cant download debate: {'/'.join(link)}")
                self.failed_links.append('/'.join(link))
                continue



            # move file to the text files folder
            new_debate_file = str(datetime.now().microsecond) + debate_file
            new_debate_file = f"{txt_files_dir}/UK/{new_debate_file}"

            try:
                os.rename(f"{download_dir}\\{debate_file}", new_debate_file)
            except FileExistsError:
                print("Collector problem while copying donwloaded file from download dir to text dir")
            except PermissionError:
                print("perm error")
                time.sleep(0.2)
                os.rename(f"{download_dir}\\{debate_file}", new_debate_file)
            else:
                yield (new_debate_file, debate_date)
            finally:
                print("err")
                continue


    def __get_members_og_ids(self, driver: uc.Chrome, final_page=0):
        url = "https://hansard.parliament.uk/search/Members?currentFormerFilter=0&startDate=01%2F01%2F2000%2000%3A00%3A00&endDate=03%2F24%2F2024%2000%3A00%3A00&partial=False"
        start_page = 1
        rep_member_id_param = re.compile(r"memberId=(\d+)")

        all_ids = []
        # driver.get(all_members_link)

        driver.get(url + f'&page={start_page}')

        try:
            n_pages = WebDriverWait(driver, 5).until( \
                EC.visibility_of_element_located(
                    (By.XPATH, '/html/body/main/div[2]/article/div/div[3]/div[2]/div/div[1]/div/strong[3]'))).text

            n_pages = int(n_pages)
            print(n_pages)
        except TimeoutException:
            print("UK_dataCollctor - get_members_og_ids: no members found for this period, maybe error with the link or the driver, or timeout")
            return []

        if final_page == 0:
            final_page = n_pages + 1
        else:
            final_page = min(final_page, n_pages + 1)
        #
        # links = []
        #
        for page in range(start_page, final_page):
            driver.get(url + f'&page={page}')
            print(f"page {page}/{n_pages}")
            search_results = WebDriverWait(driver, 30).until( \
                EC.visibility_of_any_elements_located((By.XPATH, "/html/body/main/div[2]/article/div/div[3]/div[3]/a")))
            # print("search res", search_results)
            # all_ids.extend([result.get_attribute("href") for result in search_results if result.get_attribute("href").startswith("/search/MemberContributions?memberId=")])
            for result in search_results:
                # print((result.get_attribute("href")))
                if "memberId=" in result.get_attribute("href"):
                    # print("here2")
                    curr_ids = rep_member_id_param.findall(result.get_attribute("href"))
                    if len(curr_ids) == 1:
                        all_ids.append(curr_ids[0])





        print("collected members num: ", len(all_ids))
        return all_ids


    def __get_historical(self, og_ids):
        # all_data is list of dicts of format {"name": MP name, "party": party, "from", date, "to": date}
        all_data = []

        for i, MP_id in enumerate(og_ids):
            print(f"member {i}/{len(og_ids)}")
            url = "https://members-api.parliament.uk/api/Members/History?ids"
            res = reqs.get(f"{url}={MP_id}", headers={"accept": "text/json"}).json()[0]["value"]
            # print(f"{url}={MP_id}")
            MP_name = res["nameHistory"][0]["nameDisplayAs"]
            house = res['houseMembershipHistory'][0]["house"]
            party_hist = res["partyHistory"]

            for party in party_hist:
                MP = {
                    "og_id": MP_id,
                    "name": MP_name,
                    "party": party['party']['name'],
                    "house": house,
                    "startDate": party['startDate'],
                    "endDate": party['endDate']

                }
                all_data.append(MP)
                # print("MP:", MP)
            # print("party_hist:", party_hist)


        return all_data


    def __get_session_bills_ids(self, session):
        skip = 0

        # url = f"https://bills-api.parliament.uk/api/v1/Bills?Session={session}&CurrentHouse=All&OriginatingHouse=All&Skip={skip}&Take=20"
        all_bills = []
        while True:
            url = f"https://bills-api.parliament.uk/api/v1/Bills?Session={session}&CurrentHouse=All&OriginatingHouse=All&Skip={skip}&Take=20"
            res = reqs.get(url, headers={"accept": "text/json"}).json()

            if len(res["items"]) == 0:
                break

            for bill_item in res['items']:
                all_bills.append(
                    {
                        "bill_id": bill_item["billId"],
                        "title": bill_item["shortTitle"]
                    }
                )

            skip += 20

        return all_bills


    def __get_bills_intro_date(self, bills):
        processed_bills = []

        for bill in bills:
            url = f"https://bills-api.parliament.uk/api/v1/Bills/{bill['bill_id']}/Stages"
            res = reqs.get(url, headers={"accept": "text/json"}).json()

            if len(res['items']) == 0:
                continue

            # print(res)
            intro_date = res['items'][0]['stageSittings'][0]["date"]
            bill['date'] = intro_date

            processed_bills.append(bill)

        return processed_bills

if __name__ == "__main__":
    a = UK_DataCollector(20, "test")
    links = a.get_members()
    #
    # print(links, len(links))