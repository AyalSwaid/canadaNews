from Collectors.DataCollectors.DataCollector import DataCollector
import requests as reqs
from bs4 import BeautifulSoup as bs
from Data.GLOBAL import Data
from datetime import datetime, timedelta

import pandas as pd
# Collected 25 plenum docs files in 49 seconds
# NOTE: EACH PLENUM CONTAINS MULTIPLE DEBATES
class IL_DataCollector(DataCollector):
    def __init__(self, batch_size):
        super(IL_DataCollector, self).__init__(batch_size)
    
    
    def get_debates(self):
        print("collecting IL debates")
        plenum_list = []

        # get dates range
        json_prog = Data.get_progress()
        start_date = json_prog['IL_debates_start_date']
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = start_date + timedelta(days=self.batch_size)



        for entries in self.get_plenum_bulks(start_date, end_date):
            # iterate ovcer all entries


            for entry in entries:
                is_special = entry.find('textIsSpecialMeeting')
                if is_special:
                    continue

                plenum_id = entry.find('PlenumSessionID').text
                plenum_date = entry.find('StartDate').text
                print("entry date: ", plenum_date)
                # TODO: convert date to relevant format


                # get all files for this debate
                # plenum_files format: (type, file_path)
                plenum_files = self.__get_plenum_files(plenum_id)
                if not plenum_files:
                    continue
                print("filesL",plenum_files)


                curr_plenum = {
                    "plenum_date": plenum_date,
                    "files": plenum_files
                }
                plenum_list.append(curr_plenum)


        # write batch data into disk (pass to processor)
        file_name = str(datetime.now()).replace(':', "-")
        batch_file_path = f"{Data.processor_debates_dir}/IL/{file_name}.json"
        Data.save_json(batch_file_path, plenum_list)

        # TODO: update start date for the next batch
        json_prog['IL_debates_start_date'] = end_date.strftime("%Y-%m-%d")
        Data.update_progress(json_prog)
            # print(first_element.find('properties'))
            # print(first_element.find_all('SessionUrl'))
        print("DONE IL debates")

    def get_members(self):

        all_members = []
        id2name = {}


        for entries in self.__get_members_bulks():
            for entry in entries:
                MP_id = entry.find("PersonID").text
                party_id = entry.find("FactionID").text
                start_date = entry.find("StartDate").text
                end_date = entry.find("FinishDate").text

                MP_name = id2name.get(MP_id, None)
                if MP_name is None:
                    MP_name = self.__get_member_name(MP_id)
                    id2name[MP_id] = MP_name
                # MP_name = self.__get_member_name(MP_id)

                all_members.append({
                    "name": MP_name,
                    "party_id": party_id,
                    "startDate": start_date,
                    "endDate": end_date
                })
                # print(f"party_name:{party_name}, party_id:{party_id}\n")


        pd.DataFrame(all_members).to_csv(f"{Data.csv_files_dir}/members/IL_members.csv", index=False)


        # print(entries[0])
        # print(len(entries))


    def get_bills(self):
        # define __get_bills_bullks() method

        all_bills = []
        id2name = {}

        # for each bulk and for each bill in each bulk:
        for entries in self.__get_bills_bullks():
            for entry in entries:
                # get (bill_id, title, date) and store them in dataframe then in csv
                bill_id = entry.find("BillID").text
                bill_title = entry.find("Name").text
                bill_date = entry.find("PublicationDate").text

                all_bills.append({
                    "bill_id": bill_id,
                    "title": bill_title,
                    "date": bill_date
                })



        pd.DataFrame(all_bills).to_csv(f"{Data.csv_files_dir}/bills/IL_bills.csv", index=False)


    def get_parties(self):
        all_parties = []
        id2name = {}

        # for each bulk and for each bill in each bulk:
        for entries in self.__get_parties_bulks():
            for entry in entries:
                # get (bill_id, title, date) and store them in dataframe then in csv
                party_id = entry.find("FactionID").text
                party_name = entry.find("Name").text
                party_start_date = entry.find("StartDate").text
                party_end_date = entry.find("FinishDate").text
                party_KNS_num = entry.find("KnessetNum").text

                all_parties.append({
                    "party_id": party_id,
                    "party_name": party_name,
                    "start_date": party_start_date,
                    "end_date": party_end_date,
                    "KnessetNum": party_KNS_num
                })

        pd.DataFrame(all_parties).to_csv(f"{Data.csv_files_dir}/parties/IL_parties.csv", index=False)

    def __get_plenum_files(self, plenum_id):
        """
        given plenum_id, query its related files from KNS_DocumentPlenumSession, download
        each file, save its path and type and return them as a list of tuples
        :param plenum_id:  int
        :return: donwloaded files: list of tuples of format [(file_type, file_path)]
        """
        file_types_blacklist = {"URL", "VDO"}
        files_url = 'https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_DocumentPlenumSession'
        url = f"{files_url}?$filter=PlenumSessionID eq {plenum_id}"


        files_resp = reqs.get(url)
        files_soup = bs(files_resp.content, 'xml')

        # first_element = soup.find('entry').find('properties')
        files_entries = files_soup.find_all('entry')

        # iterate over all related files
        files = []
        for file_entry in files_entries:
            file_type = file_entry.find("ApplicationDesc").text
            if file_type in file_types_blacklist:
                continue

            group_type_desc = file_entry.find("GroupTypeDesc").text # this contains wither it is a debate or just table of contents
            file_path_url = file_entry.find("FilePath").text

            # filter non relevant files (keep only debates)
            if (group_type_desc != "דברי הכנסת") or ( "_toc_" in file_path_url): # TODO: maybe ask shai about these
                continue

            print("file url:", file_path_url, file_type)
            response = reqs.get(file_path_url)

            file_path = f"{Data.text_files_dir}/IL/{file_path_url.split('/')[-1]}"
            if response.status_code == 200:
                #TODO: sometimes downloading the file raises exception
                # 1. check if file already exist
                # 2. sometimes status code is 200 but the page is error page
                # Open the file in binary write mode and write the content of the response
                with open(file_path, "wb") as f:
                    f.write(response.content)
            else:
                print("Failed to download file. Status code:", response.status_code)
                continue

            files.append((file_type, file_path))



        return files


    def get_plenum_bulks(self, start_date, end_date):
        # Define urls for the OData
        # TODO: SET CORRECT FILTER
        debates_url = f"""https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_PlenumSession?$filter=StartDate ge datetime'{start_date.strftime("%Y-%m-%d")}T00:00' and StartDate le datetime'{end_date.strftime("%Y-%m-%d")}T23:59'"""
        print(f"getting {debates_url}")

        skip_size = 100
        curr_bulk = 0

        # get OData output
        entries = ['tmp']
        while entries:
            resp = reqs.get(f"{debates_url}&$skip={skip_size*curr_bulk}")
            soup = bs(resp.content, 'xml')
            entries = soup.find_all('entry')
            print("LENS:",len(entries))
            yield entries
            curr_bulk += 1


    def __get_members_bulks(self):
        url = "https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_PersonToPosition?"



        skip_size = 100
        curr_bulk = 0

        # get OData output
        entries = ['tmp']
        while entries:
            print(f"BULK: {curr_bulk}/ 110")
            resp = reqs.get(f"{url}$skip={skip_size * curr_bulk}")
            soup = bs(resp.content, 'xml')
            entries = soup.find_all('entry')

            yield entries
            curr_bulk += 1


    def __get_member_name(self, MP_id):
        url = f"https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_Person({MP_id})/"

        resp = reqs.get(url)
        soup = bs(resp.content, 'xml')

        first_name = soup.find("FirstName").text
        last_name = soup.find("LastName").text
        return first_name + " " + last_name


    def __get_bills_bullks(self):
        url = "https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_Bill?"

        skip_size = 100
        curr_bulk = 0

        # get OData output
        entries = ['tmp']
        while entries:
            print(f"BULK: {curr_bulk}/ 563")
            resp = reqs.get(f"{url}$skip={skip_size * curr_bulk}")
            soup = bs(resp.content, 'xml')
            entries = soup.find_all('entry')
            yield entries
            curr_bulk += 1


    def __get_parties_bulks(self):
        url = "https://knesset.gov.il/Odata/ParliamentInfo.svc/KNS_Faction?"

        skip_size = 100
        curr_bulk = 0

        # get OData output
        entries = ['tmp']
        while entries:
            print(f"BULK: {curr_bulk}/ 55")
            resp = reqs.get(f"{url}$skip={skip_size * curr_bulk}")
            soup = bs(resp.content, 'xml')
            entries = soup.find_all('entry')
            yield entries
            curr_bulk += 1


if __name__ == "__main__":
    a = IL_DataCollector(20)
    # a.get_debates()
    a.get_members()