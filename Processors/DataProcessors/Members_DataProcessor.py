import re
import os
import pandas as pd

from Data.GLOBAL import Data
from Processors.DataProcessors.DataProcessor import DataProcessor

class Members_DataProcessor(DataProcessor):
    def __init__(self, batch_size):
        super(Members_DataProcessor, self).__init__(batch_size)

        self.table = "bills"

    
    def to_csv(self):
        pass


    def process_data(self):
        pass
    

    def process_UK(self):
        pass


    def process_IL(self):
        pass


    def process_USA(self):
        pass


    def process_TN(self):
        print("processor (TN members) started")
        dir_path = f"{Data.processor_dir}/members/TN"
        file_path = os.listdir(dir_path)

        if len(file_path) > 0:
            file_path = file_path[0]
        else:
            print('processor (IL debates) did not find files to process')
            return

        members_table = [] # list of dicts save this later as csv
        members = Data.load_json(dir_path + "/" + file_path)
        for MP_name, party_periods in members.items():
            for party_period in party_periods:
                start_date, end_date = self.__parse_period(party_period[1])
                curr_row = {
                    "name": MP_name,
                    "party": party_period[0], # TODO: this is party name ... convert it to party id
                    "startDate": start_date,
                    "endDate": end_date
                }

                members_table.append(curr_row)

        csv_file_path = f"{Data.csv_files_dir}/members/TN_members.csv.csv"
        pd.DataFrame(members_table).to_csv(csv_file_path)



    def process_CA(self):
        pass

    def __parse_period(self, periods):
        periods = periods.replace("\n", "").split("-")

        rep_month = re.compile(r"[^\d]+")
        rep_year = re.compile("\d+")
        month_ar2num = {
            "جانفي": 1,
            "فيفري": 2,
            "مارس": 3,
            "أفريل": 4,
            "ماي": 5,
            "جوان": 6,
            "جويلية": 7,
            "أوت": 8,
            "سبتمبر": 9,
            "أكتوبر": 10,
            "نوفمبر": 11,
            "ديسمبر": 12
        }
        for i in range(len(periods)):
            # deal with "until now" and set it as None
            if periods[i] == "الآن":
                periods[i] = None
                continue

            year, month = rep_year.findall(periods[i])[0], rep_month.findall(periods[i])[0]
            # convert month into number
            periods[i] = f"{year}-{month_ar2num[month]}-01"


        return periods[0], periods[1]



if __name__ == "__main__":
    a = Members_DataProcessor(20)
    a.process_TN()