from Collectors.DataCollectors.DataCollector import DataCollector
import requests
import json
from datetime import datetime, timedelta
from Data.GLOBAL import Data


# TODO check for next url
class USA_DataCollector(DataCollector):
    def __init__(self, batch_size):
        super(USA_DataCollector, self).__init__(batch_size)
        self.url = 'https://api.govinfo.gov/search'
        self.api_key = 'c2mQmLAgAYvSIawOm9aPWLr2kYs277VUxqz6DS9L'
        self.PROPUBLICA_API_KEY = '1VLppuDeNqAKZ02Ii6QYvURffxMNANYgCnuMrt77'

    def get_debates(self):
        json_prog = Data.get_progress()
        start_date = json_prog["USA_debates_start_date"]
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = start_date + timedelta(days=self.batch_size)

        search_query = {
            "query": f"Congressional Record -(daily and digest) publishdate:range({start_date.strftime('%Y-%m-%d')},{end_date.strftime('%Y-%m-%d')})",
            "pageSize": 1000,
            "offsetMark": "*",
            "sorts": [
                {
                    "field": "score",
                    "sortOrder": "DESC"
                }
            ]
        }

        headers = {'X-Api-Key': self.api_key}
        query_response = requests.post(self.url, json=search_query, headers=headers)

        if query_response.status_code == 200:
            data = query_response.json()
            if data:
                json_file_name = str(datetime.now()).replace(':', "-")
                with open(f'{Data.processor_debates_dir}/USA/{json_file_name}.json', 'w') as f:
                    json.dump(data, f)

        # print(start_date, end_date)
        json_prog["USA_debates_start_date"] = end_date.strftime("%Y-%m-%d")
        Data.update_progress(json_prog)

    def get_members(self):
        # Define the headers with the API key
        json_prog = Data.get_progress()
        congress = json_prog["USA_members_congress_number"]
        headers = {
            'X-API-Key': self.PROPUBLICA_API_KEY
        }

        chambers = ["house", "senate"]
        all_data = {}

        for chamber in chambers:

            url = f"https://api.propublica.org/congress/v1/{congress}/{chamber}/members.json"
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if data:
                    all_data[chamber] = data
        json_file_name = f"{congress}_{str(datetime.now()).replace(':', '-')}"
        with open(f'{Data.processor_members_dir}/USA/{json_file_name}.json', 'w') as f:
            json.dump(all_data, f)  # TODO save the last date

        json_prog["USA_members_congress_number"] = str(eval(congress) + 1)
        Data.update_progress(json_prog)

    def get_bills(self):
        # Define the base URL and endpoint for bills
        json_prog = Data.get_progress()
        start_date = json_prog["USA_bills_start_date"]
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = start_date + timedelta(days=self.batch_size)

        api_url = f"https://api.govinfo.gov/published/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"

        params = {
            "offset": 0,
            "pageSize": 1000,  # Number of results per page
            "collection": "BILLS,BILLSTATUS",
            "docClass": "hr,s",
            "api_key": self.api_key
        }

        # Make the GET request
        response = requests.get(api_url, params=params)

        if response.status_code == 200:
            data = response.json()
            if data["packages"]:
                json_file_name = str(datetime.now()).replace(':', "-")
                with open(f'{Data.processor_bills_dir}/USA/{json_file_name}.json', 'w') as f:
                    json.dump(data, f)
            json_prog["USA_bills_start_date"] = end_date.strftime("%Y-%m-%d")
            Data.update_progress(json_prog)