import json
import os
import pickle
import spacy


"""
Here are global variables that contain information like files paths, csv paths, etx...
dont change the value of any of these variables
"""
print("GLOBAL.py is loading spacy nlp, it may take some time")
nlp = spacy.load("en_core_web_sm")
print("DONE loading")

class Data:
    processor_dir = 'Data/to_process'
    processor_debates_dir = 'Data/to_process/debates'
    processor_bills_dir = 'Data/to_process/bills'

    csv_files_dir = 'Data/csv_files'
    speeches_files_dir = 'Data/speeches'


    progress_json = 'Data/progress.json'

    # chrome_driver_path = r'C:\Users\ayals\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe'
    chrome_driver_path = r'C:\Users\ayals\OneDrive\שולחן העבודה\parliamentMining\chromedriver-win64\chromedriver-win64\chromedriver.exe'
    text_files_dir = 'Data/tmp_text_files'

    failed_links_pkl = 'Data/failed_links.pkl'

    country2code = {
        "USA": 1,
        "uk": 2,
        "IL": 3,
        "CA": 4,
        "TN": 5,
    }

    code2country = {v:k for k,v in country2code.items()}

    @staticmethod
    def get_progress():
        with open(Data.progress_json, "r") as json_file:
            my_dict = json.load(json_file)

        return my_dict


    @staticmethod
    def update_progress(new_dict):
        with open(Data.progress_json, 'w') as file:
            json.dump(new_dict, file, indent=4)

    @staticmethod
    def load_pkl(file_path):
        with open(file_path + '.pkl', 'rb') as f:
            x = pickle.load(f)
        return x

    @staticmethod
    def save_pkl(obj, file_name):
        with open(file_name + '.pkl', "wb") as f:
            pickle.dump(obj, f)

    @staticmethod
    def load_json(file_path):
        with open(file_path, "r") as json_file:
            my_dict = json.load(json_file)

        return my_dict


    @staticmethod
    def save_json(file_path, dict_obj):
        with open(file_path, 'w') as file:
            json.dump(dict_obj, file, indent=4)


    @staticmethod
    def update_failed_links(links):
        try:
            old_links = Data.load_pkl(Data.failed_links_pkl)
            os.remove(Data.failed_links_pkl)
        except:
            old_links = []

        old_links.extend(links)
        Data.save_pkl(old_links, Data.failed_links_pkl)


