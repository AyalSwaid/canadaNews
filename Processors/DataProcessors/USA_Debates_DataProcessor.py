import time
from datetime import datetime
from Processors.DataProcessors.DataProcessor import DataProcessor
import pickle
import os
import json
import pandas as pd
from Data.GLOBAL import Data
from Collectors.DataCollectors.CA_DataCollector import CA_DataCollector
from selenium.webdriver.chrome.service import Service
import csv
import re
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import requests
from selenium.webdriver.support.wait import WebDriverWait
import re
from bs4 import BeautifulSoup
import json
import csv


class Debates_DataProcessor(DataProcessor):
    def __init__(self, batch_size):
        super(Debates_DataProcessor, self).__init__(batch_size)
        self.data_path = Data.processor_debates_dir
        self.table = "debates"
        self.driver = None


    def init_driver(self):
        if self.driver is not None:
            return self.driver

        options = Options()
        options.headless = False
        # block images and javascript requests
        # chrome_prefs = {
        #     "profile.default_content_setting_values": {
        #         "images": 2,
        #         "javascript": 2,
        #     }
        # }
        # options.experimental_options["prefs"] = chrome_prefs
        # options.add_argument('--proxy-server=http://{}:{}'.format("172.183.241.1", 8080))

        service = Service(executable_path=Data.chrome_driver_path)
        driver = webdriver.Chrome(options=options, service=service)
        self.driver = driver

        return driver


    def to_csv(self):
        pass

    def process_data(self):
        pass

    def process_UK(self):
        file_path = os.listdir(self.data_path + '/UK')

        # if dir is empty then exit
        if len(file_path) > 0:
            file_path = file_path[0]
        else:
            print('processor (UK debates) did not find files to process')
            return
        # for file_path in os.listdir(self.data_path + '\\UK'):
        # NOTE: each pickle file is a single batch
        # load pickle file that contains all the debates_dates and files paths

        debates = self.load_pkl(self.data_path + '\\UK\\' + file_path)

        # iterate over the pkl and call extract_debate_data and split_members for each debate
        for debate in debates:
            # debate_title, members = self.extract_debate_data(debate['file_path'])
            debate_title, speeches = self.split_members(debate['file_path'])
            debate['debate_title'] = debate_title

            # save speeches in json
            # slice file_path[24:] to remove the ".pkl" at the end
            speeches_file_path = f"{Data.speeches_files_dir}/UK/{debate['file_path'][24:-4]}.json"
            with open(speeches_file_path, 'a+') as json_file:
                json.dump(speeches, json_file)

            debate['file_path'] = speeches_file_path

        # save debates in a csv table save
        # og_debates_table = pd.read_csv('debates.csv')
        new_debates = pd.DataFrame(debates)

        # og_debates_table = pd.concat([og_debates_table, new_debates], axis=0)
        new_debates.to_csv(f'{Data.csv_files_dir}/debates/{file_path}.csv')

        # delete pickle file
        os.remove(self.data_path + '\\UK\\' + file_path)

    def process_IL(self):
        pass

    def process_USA(self):
        DRIVER_PATH = r"C:\Users\Jiana\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"
        file_path = os.listdir(self.data_path + '/USA')

        if len(file_path) >= 1:
            data = Data.load_json(self.data_path + '/USA/' + file_path[0])
        else:
            print("No data to process")
            return

        start_idx = Data.get_progress()["USA_debates_processor_idx"]

        j = 0
        to_write = []
        counter = 0  # TODO save it somewhere
        for debate in data["results"][start_idx::]:
            date, title, country, speeches_file_path, members, speeches = "", "", 1, "", [], {}

            print(f"process {j+start_idx} from {len(data['results'])}")
            debate_id = counter
            counter += 1
            summary_link = debate['resultLink'] + "?api_key=21Q15esV5Oew1sJ9S8m9gKLaoMEe3rAYlMj4lRDn"
            try:
                summary_response = requests.get(summary_link)
            except Exception as e:
                print("blocked")
                time.sleep(60*15)
                pd.DataFrame(to_write, columns=["date", "debate_title", "country", "members", "file_path"]).to_csv(f"{Data.csv_files_dir}/debates/{file_path[0]}.csv")
                to_write = []
                summary_response = requests.get(summary_link)



            if summary_response.status_code == 200:
                summary_data = summary_response.json()
                if summary_data.get("members", False) and len(
                        summary_data["members"]) > 2:  # If at least there is two talking memberz
                    print("found deabate with 2")
                    details = summary_data[
                                  "detailsLink"] + "?api_key=21Q15esV5Oew1sJ9S8m9gKLaoMEe3rAYlMj4lRDn"

                    # options = Options()
                    # options.headless = True
                    # # block images and javascript requests
                    # # chrome_prefs = {
                    # #     "profile.default_content_setting_values": {
                    # #         "images": 2,
                    # #         "javascript": 2,
                    # #     }
                    # # }
                    # # options.experimental_options["prefs"] = chrome_prefs
                    # service = Service(executable_path=Data.chrome_driver_path)
                    # driver = webdriver.Chrome(options=options, service=service)
                    driver = self.init_driver()
                    print(details)
                    try:
                        driver.get(details)
                    except Exception as e:
                        print("ERRORED HERE: ", e)
                        driver.quit()
                        self.driver = None
                        continue
                    driver.implicitly_wait(2)
                    driver.refresh()
                    try:
                        driver.implicitly_wait(5)
                        print("done waiting")
                        element = driver.find_element(By.CSS_SELECTOR,
                            "#accMetadata > div:nth-child(11) > div.col-xs-12.col-sm-9 > p")
                        sub_type = element.text
                        driver.quit()
                        self.driver = None

                    except Exception as e:
                        print(e)
                        print("Element not found")
                        sub_type = None

                        # json_file_name = str(datetime.now()).replace(':', "-")
                        # with open(
                        #         fr'C:\Users\Jiana\OneDrive\Desktop\ParliamentMining\DataPipeline\Data\Failedlinksusa\{json_file_name}.json',
                        #         'w') as f:
                        #
                        #     json.dump(details, f)
                        #     continue

                    # TODO edit format to dict
                    if sub_type != "Honoring" and sub_type != "Celebrations" and sub_type != 'Recognitions':  # Not debates

                        date = summary_data["dateIssued"]

                        title = summary_data["title"]

                        record_link = summary_data["download"][
                                          "txtLink"] + "?api_key=21Q15esV5Oew1sJ9S8m9gKLaoMEe3rAYlMj4lRDn"
                        try:
                            txt_response = requests.get(record_link)
                        except:
                            print("Max retries exceeded with url:", record_link)
                            continue

                        # Check if the request was successful
                        if txt_response.status_code == 200:
                            # Parse the HTML content
                            soup = BeautifulSoup(txt_response.content, 'html.parser')
                            # Find and extract the text content
                            text_content = soup.get_text()
                        if text_content:
                            speeches = self.get_speech(text_content)  # this is generator

                            speeches = self.clean_speech(speeches)

                        members = summary_data[
                            "members"]  # example  'members': [{'role': 'SPEAKING',
                        # 'chamber': 'S',
                        # 'congress': '118',
                        # 'bioGuideId': 'S000033',
                        # 'memberName': 'Sanders, Bernard',
                        # 'state': 'VT',
                        # 'party': 'I'}]

                        members = self.get_members_USA(members)
                        country = 1
                        speeches_file_path = f"{Data.speeches_files_dir}/USA/{counter}_{file_path[0]}.json"
                        if speeches:
                            with open(speeches_file_path, 'a+') as json_file:
                                json.dump(speeches, json_file)
                            to_write.append(
                                [date, title, country, speeches_file_path,
                                 members])
                else:

                    counter -= 1
            else:
                print(summary_response.status_code)
                continue
            j += 1

        # exit driver, this line also exits all opened chrome instances
        os.system("taskkill /F /IM chrome.exe")
        self.driver = None

        csv_file_path = f"{Data.csv_files_dir}/debates/{file_path[0]}.csv"
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["date", "debate_title", "country", "file_path", "members"])
            writer.writeheader()

            writer = csv.writer(csvfile)
            writer.writerows(to_write)

        if os.path.exists(self.data_path + '/USA/' + file_path[0]):
            os.remove(self.data_path + '/USA/' + file_path[0])

    def process_TN(self):
        pass

    def process_CA(self):

        file_path = os.listdir(self.data_path + '/CA')
        if len(file_path) >= 1:
            data = Data.load_json(self.data_path + '/CA/' + file_path[0])
        else:
            print("No data to process")
            return

        output = []
        out = {}
        i = 0
        members = set()
        for item in data:
            for debate in item:  # jalse w7de
                date = debate["date"]
                link = "https://api.openparliament.ca" + debate["url"]
                debate_data = CA_DataCollector.send_request(link)
                speeches = "https://api.openparliament.ca" + debate_data["related"]["speeches_url"]
                speeches = self.get_all(url=speeches)  # get all speeches for one jalse
                for x in speeches:  # all speeches in jalse w7de
                    for speech in x:  # all speeches in link wa7d
                        if speech.get("h1", None):
                            if speech["h1"]["en"] == "Government Orders":
                                speaker = self.get_member_CA(speech["politician_url"])
                                flag = speech.get("h2", "")
                                if flag:
                                    title = speech["h2"]["en"]
                                else:
                                    print(speech)
                                    title = ""
                                content = speech["content"]["en"]
                                content = re.sub(r'<[^>]*>|[\n\r]', '', content)
                                out[title] = out.get(title, [])
                                out[title].append(
                                    {"name": speaker, "speech": content})  # find for each debate ( title) the speeches
                # load to DB out( each item in out is debate) , members , date , counter
                # not out contains debates in jalse w7de ,  out is dict that has the
                # title of debate and all the speeches

                for title, speeches in out.items():
                    # print(speeches)
                    # print("stop")
                    for speech in speeches:
                        name = list(speech.keys())
                        if name[0] != "":
                            members.add(name[0])
                    speeches_file_path = f"{Data.speeches_files_dir}/CA/{str(datetime.now()).replace(':', '-')}_{file_path[0]}.json"
                    with open(speeches_file_path, 'a+') as json_file:
                        json.dump(speeches, json_file)
                    output.append([date, title, 4, members, speeches_file_path])
                    i += 1
                    out = {}
                    members = set()
        csv_file_path = f"{Data.csv_files_dir}/debates/{file_path[0]}_{str(datetime.now()).replace(':', '-')}.csv"
        # json_file_name = str(datetime.now()).replace(':', "-")
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["date", "debate_title", "country", "members", "file_path"])
            writer.writeheader()

            writer = csv.writer(csvfile)
            writer.writerows(output)
        if os.path.exists(self.data_path + '/CA/' + file_path[0]):
            os.remove(self.data_path + '/CA/' + file_path[0])

        print("CSV file has been created successfully.")

    def get_member_CA(self, link):
        member_data = None
        if link:
            member_data = CA_DataCollector.send_request("https://api.openparliament.ca" + link)
        if member_data:
            return member_data["name"]
        return ""

    def get_all(self, url):
        all_data = []
        data = CA_DataCollector.send_request(url)
        curr_data = data["objects"]  # send curr in function to load data
        next_data = data["pagination"]["next_url"]
        all_data.append(curr_data)
        while True:
            if next_data:
                next_url = "https://api.openparliament.ca" + next_data
                new_data = CA_DataCollector.send_request(next_url)
                all_data.append(new_data["objects"])
                next_data = new_data["pagination"]["next_url"]
            else:
                break
        return all_data

    def extract_debate_data(self, file_path):
        '''
        given debate txt file path, process and extract data like debate title, members,
        :file_path: string, full path of the debate txt file
        :return: debate_title: string
                members: list
        '''
        members = set()

        with open(file_path, "r") as file:
            lines = file.read().split('\n')
            debate_title = lines[0].strip()

            for i in range(1, len(lines)):
                # print(lines[i])
                # according to the txt files format, when new member speaks the next line is not '\n'
                if (lines[i] != '') and (lines[i - 1] != ''):  # New member is taling
                    members |= set([lines[i - 1]])

            return debate_title, list(members)

    def split_members(self, file_path):
        '''
        take debates text file and extract member name and speeches out of them
        :param files_path:
        :return:
        '''
        # files_path = os.listdir(txt_files_dir)

        members = set()
        speeches = {}

        # for file_path in files_path:
        curr_debate_speeches = {}

        with open(file_path, "r") as file:
            lines = file.read().split('\n')
            debate_title = lines[0].strip()
            speech = ''
            member_name = None
            for i in range(1, len(lines)):

                # print(lines[i])
                # according to the txt files format, when new member speaks the next line is not '\n'
                if (lines[i] != '') and (lines[i - 1] != ''):  # New member is talking
                    if member_name is not None:
                        curr_debate_speeches[member_name] = curr_debate_speeches.get(member_name, '') + speech
                    member_name = lines[i - 1]
                    members |= set([member_name])
                    speech = lines[i]
                else:
                    speech += lines[i] + ' '
            speeches[debate_title] = curr_debate_speeches

        return debate_title, speeches

    def load_pkl(self, file_path):
        with open(file_path, 'rb') as file:
            data = pickle.load(file)

        return data

    def get_speech(self, text):

        output = []

        # Correcting the regex pattern for speaker names
        SPEAKER_REGEX = re.compile(r"(?:^  +M[rs]\. [a-zA-Z-]+(?: |\.))|(?:^  +The [A-Z ]{2,}\.)", re.MULTILINE)
        current_speaker = None
        speech_start = None

        for m in re.finditer(SPEAKER_REGEX, text):
            name_start = m.start(0)
            name_end = m.end(0)

            if current_speaker is not None:
                speech_end = name_start
                output.append({"name": current_speaker, "speech": text[speech_start:speech_end].strip()})

            speech_start = name_end
            current_speaker = text[name_start:name_end].strip()[:-1]

        if current_speaker is not None:
            output.append({"name": current_speaker, "speech": text[speech_start:].strip()})

        return output

    def clean_speech(self, lst):
        new = []
        pattern = r'\[\[Page [SH]?\d*\]\]'

        for entry in lst:
            name = entry.get("name", "")
            text = entry.get("speech", "").replace("\n", "")
            text = re.sub(pattern, '', text)  # Remove page patterns
            text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces with a single space
            text = re.sub(r"<[^>]*>", '', text)  # Remove HTML tags
            text = re.sub(r"_{2,}", ' ', text)  # Replace multiple underscores with a single space

            new.append({"name": name, "speech": text.strip()})  # Add cleaned entry to the new list

        return new

    def get_members_USA(self, members_lst):
        members = {}
        for i in members_lst:
            if i.get("memberName", None) and i.get("bioGuideId"):
                name = i["memberName"].replace(', ', ' ')
                members[i["bioGuideId"]] = name
            else:  # TODO sometimes output like this {'role': 'SPEAKING', 'chamber': 'H', 'congress': '111'}
                print(i)
        return members