import os

from Data.GLOBAL import Data
from Collectors.CollectorManager import CollectorManager
from Processors.ProcessorManager import  ProcessorManager
from Collectors.NewsCollectors.test_ca_multiprocessing import CA_NewsCollector
import json
from time import time
import time as t

# import undetected_chromedriver as uc
#
# d_path = r'C:\Users\ayals\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe'
# uc.Chrome(version_main=120, driver_executable_path=d_path)
"""
stats USA:
seconds | num of debates | seconds per one debate
4775    | 336            | 14   
"""
if __name__ == "__main__":
    collector_m = CollectorManager(10)
    processor_m = ProcessorManager(10)
    ca_news = CA_NewsCollector(10)
    #
    since = time()
    for i in range(3):# 1
        print(f"batch {i+1}") # TODO: make unique csv file name in tunis processor - PermissionError: [Errno 13] Permission denied: 'Data/csv_files/debates/2024-06-15-03-13-08.json.csv'
        # collector_m.run_collectors()
        # processor_m.run_processors()
        ca_news.get_news()
        print("sleeping, copy tmp text files")
        t.sleep(2)
        # for p in os.listdir(Data.text_files_dir+'/USA'):
        #     os.remove(f'{Data.text_files_dir}/USA/{p}')

    print(f"elapsed: {time()-since}")




