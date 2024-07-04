from Processors.DataProcessors.DataProcessor import DataProcessor
import pickle
import os
from fuzzywuzzy import process
import json
import pandas as pd
from Data.GLOBAL import Data
# from docx.api import Document
import win32com.client # pip install pywin32
import Data.nlph_utils as nlph
from Data.GLOBAL import nlp
from collections import defaultdict
# from time import time
from datetime import datetime
import re
import spacy


"""
TODO LIST:
5. extract the exact correct members name from text files
6. do scraping function to scrape data about members.
7. take into account that there might be non-MP members in debates
8. github: hansard open gov
9. make sub-folders in text_files folder to make it easier for the file system
"""

"""
Statistics summary:
IL: 25 plenums -> 966 debates -> 15 minutes -> rate= 1 debate per second
"""

class Debates_DataProcessor(DataProcessor):
    def __init__(self, batch_size):
        super(Debates_DataProcessor, self).__init__(batch_size)
        self.data_path = Data.processor_debates_dir
        self.table = "debates"
        self.word = None # used for IL

        self.members = None # for debates_members table processing



    def __del__(self):
        if self.word is not None:
            self.word.Quit()
    
    def to_csv(self):
        pass


    def process_data(self):
        pass
    

    def process_UK(self):

        file_path = os.listdir(self.data_path + '/UK')
        print(file_path)

        # if dir is empty then exit
        if len(file_path) > 0:
            file_path = file_path[0]
        else:
            print('processor (UK debates) did not find files to process')
            return
        # for file_path in os.listdir(self.data_path + '\\UK'):
        # NOTE: each pickle file is a single batch
        # load pickle file that contains all the debates_dates and files paths

        # load pickle file that is a list of dicts contains a date and txt file path of each debate
        debates = self.load_pkl(self.data_path + '/UK/'+file_path)

        # here we store new debates metadata for csv
        all_debates = []

        # iterate over the pkl and call extract_debate_data and split_members for each debate
        i = 0
        print(f"Processor (UK debates) started to process {len(debates)} debates")
        for debate in debates:
            # debate_title, members = self.extract_debate_data(debate['file_path'])
            # debate_title, speeches = self.UK_split_members(debate['file_path'])
            debate_title, speeches = self.test_UK_split_members(debate['content_file_path'])

            if len(speeches) < 2:
                # print(f"small debate: {debate['file_path']}")
                i += 1
                continue

            debate['debate_title'] = debate_title
            debate['country'] = 2

            # save speeches in json
            # slice file_path[24:] to remove the ".pkl" at the end
            speeches_file_path = f"{Data.speeches_files_dir}/UK/{debate['content_file_path'][24:34]}W{str(datetime.now()).replace(':', '-')}.json"

            Data.save_json(speeches_file_path, speeches)

            # with open(speeches_file_path, 'a+') as json_file:
            #     json.dump(speeches, json_file)

            # debate['file_path'] = speeches_file_path
            debate = {
                "date": debate["debate_date"],
                "debate_title": debate_title,
                "country": 2,
                "file_path": speeches_file_path
            }
            all_debates.append(debate)
        print(f"DONE, skipped {i} empty debates")
        # save debates in a csv table save
        # og_debates_table = pd.read_csv('debates.csv')
        new_debates = pd.DataFrame(all_debates)

        # og_debates_table = pd.concat([og_debates_table, new_debates], axis=0)
        new_debates.to_csv(f'{Data.csv_files_dir}/debates/{file_path}.csv')

        # delete pickle file
        os.remove(self.data_path + '/UK/'+file_path)




    def process_IL(self):
        print("processor (IL debates) started")
        file_path = os.listdir(self.data_path + '/IL')

        # TODO: put blacklist of files that cause errors
        # if dir is empty then exit
        if len(file_path) > 0:
            file_path = file_path[0]
        else:
            print('processor (IL debates) did not find files to process')
            return

        # file_path = "tor_test.json" # TODO: delete this

        plenums = Data.load_json(f"{Data.processor_debates_dir}/IL/{file_path}")

        if self.word is None:
            self.__init_wordApp()

        all_debates = []
        p_num = 1
        for plenum in plenums:
            print(f"plenum {p_num}/{len(plenums)}")
            p_num += 1

            files = plenum['files']
            date = plenum['plenum_date']

            # debates = self.__process_IL_files(files)
            # iterate over generator returned from __process_IL_files(files)
            for debates in self.__process_IL_files(files):

                d_num = 1
                # print("<PROCESS_IL FUNCTION -> debates:\n", debates)

                # each generator step returns a list of debates of format [{"debate_title": title, "speeches": speeches}]
                for i, debate in enumerate(debates):
                    print(f"\tdebate {d_num}/{len(debates)}")
                    d_num += 1

                    curr_debate = {"date": date, "debate_title": debate['debate_title'], "country": 3}

                    # save speeches in json
                    filename = f"{i}m{datetime.now().strftime('%Y-%m-%d %H-%M-%S')}.json"
                    Data.save_json(f"{Data.speeches_files_dir}/IL/{filename}", debate['speeches'])# TODO: uncomment this


                    #
                    curr_debate['file_path'] = f"{Data.speeches_files_dir}/IL/{filename}"
                    all_debates.append(curr_debate)


            # save all debates in csv file as a batch
            new_debates = pd.DataFrame(all_debates)

            # og_debates_table = pd.concat([og_debates_table, new_debates], axis=0)
            new_debates.to_csv(f'{Data.csv_files_dir}/debates/{file_path.split("/")[-1]}.csv') # TODO: uncomment
            # new_debates.to_csv(f'testingSpeeches/{file_path.split("/")[-1]}.csv') # TODO: delete

            # TODO: remove file from collector

        # TODO: remove file fro to process dir
        os.remove(Data.processor_debates_dir + '/IL/' + file_path)
        if self.word is not None:
            self.word.Quit()
            self.word = None

    def process_USA(self):
        pass


    def process_TN(self):
        print("processor (TN debates) started")
        all_debates = []# this is saved as csv at the end
        file_path = os.listdir(self.data_path + '/TN')

        # TODO: put blacklist of files that cause errors
        # if dir is empty then exit
        if len(file_path) > 0:
            file_path = file_path[0]
        else:
            print('processor (IL debates) did not find files to process')
            return

        debates_files = Data.load_json(self.data_path + '/TN/' + file_path)

        for idx, debate_file in enumerate(debates_files):
            # print("TITLE:", debate_file['debate_title'])
            # check if the debate is of format after/before 2019
            if debate_file["periodID"] == 1: # before 2019
                speeches = self.__TN_get_speeches2014(debate_file['data'])
            elif debate_file['periodID'] == 2: # after 2019
                speeches = self.__TN_get_speeches2019(debate_file['data'])
            else:
                print("invalid periodID: ", debate_file["periodID"])
                continue

            if not speeches:
                continue

            speech_file_path = f"{Data.speeches_files_dir}/TN/{idx}om{datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')}.json"
            Data.save_json(speech_file_path, speeches)

            all_debates.append(
                {
                    "date": debate_file['date'],
                    "file_path": speech_file_path,
                    "debate_title": debate_file['debate_title'],
                    "country": Data.country2code['TN']
                }
            )

        csv_file_path = f"{Data.csv_files_dir}/debates/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')}.csv"
        pd.DataFrame(all_debates).to_csv(csv_file_path)
        os.remove(Data.processor_debates_dir + '/TN/' + file_path)


    def process_CA(self):
        pass


    def extract_UK_debate_data(self, file_path):
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

    def test_UK_split_members(self, file_path):
        # members = set()
        # speeches = defaultdict(str)  # format: {member_name: speech, member_name: speech}

        # the following list contains all speehes (larger than 5 words) in the same order.
        # it contains dicts of format {"name": member_name-str, "speech": member's speech}
        all_speeches_list = []


        # for file_path in files_path:
        curr_debate_speeches = {}
        rep_speakers = re.compile(r".+\n.+")
        rep_speech = re.compile(r".+")
        # r""

        with open(file_path, "r") as file:
            # lines = file.read().split('\n')
            lines = file.read()
            debate_title = lines.split("\n")[0].strip()

        # lines = ""
        # print(lines)
        matches = [match for match in rep_speakers.finditer(lines)] # put it in a list to use indexing
        speech_idx = 0
        for i in range(len(matches)):
            # print(f"\tspeech {i}")
            start, end = matches[i].start(), matches[i].end()
            match = lines[start:end]
            splitted_match = match.split("\n")
            speech_end_idx = matches[i+1].start() if i < (len(matches)-1) else len(lines)
            
            speaker = splitted_match[0].strip() # now speaker noctains name and party, position, etc. example: "Afzal Khan (Manchester, Gorton) (Lab)"
            speaker = self.__extract_uk_speaker_name(speaker)


            # note that match includes speaker name and his next first paragraph
            speech = lines[start+len(splitted_match[0]) : speech_end_idx]
            speech = " ".join(rep_speech.findall(speech))

            if len(speech) > 5:
                # speeches[speaker] += f"<{speech_idx}>{speech}"

                curr_speech = {
                    "name": speaker,
                    "speech": speech
                }
                all_speeches_list.append(curr_speech)
                speech_idx += 1

        return debate_title, all_speeches_list







    def UK_split_members(self, file_path):
        '''
        take debates text file and extract member name and speeches out of them
        :param file_path: path
        :return: debate_title: str, speeches: dict
        '''
        # files_path = os.listdir(txt_files_dir)

        members = set()
        speeches = {}  # format: {member_name: speech, member_name: speech}

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



    def __process_IL_files(self, files):
        """
        given files list related to a single plenum, extract all the debates data
        usually it is only one doc / docx file for each plenum
        :param files: list of format [file_type: str, file_path: str], this list is made by IL Collector
        :return: debates generator, for each file in plenum file return the debates it contains
        """
        for file_type, file_path in files:
            if file_type.lower() not in ["doc", "docx"]:
                print(f"debates processor (IL) cannot process file of type {file_type}, path: {file_path}")
                continue


            # convert word to text
            texts_lines = self.__word2text(file_path)

            # extract title and speeches from texts
            # tor files contains parts of debates so we deal with them differently
            if "_tor_" in file_path:
                debates = self.__parse_IL_TOR_plenum(texts_lines)
            else:
                debates = self.__parse_IL_plenum(texts_lines)

            yield debates


    def __init_wordApp(self):
        if self.word is None:
            # self.word = win32com.client.gencache.EnsureDispatch('Word.Application')
            self.word = win32com.client.dynamic.Dispatch('Word.Application')

    def __is_paragraph_centered(self, paragraph):
        """
        This func is FUNC FOR IL WORD PROCESSING, check if given paragraph is positioned in the center
        :param paragraph: word document object paragraph
        :return: bool
        """
        alignment = paragraph.Format.Alignment
        return alignment == 1  # 1 corresponds to center alignment


    def __is_underlined(self, paragraph):
        return paragraph.Range.Font.Underline != 0


    def __is_bold(self, paragraph):
        return paragraph.Range.Font.Bold != 0

    def __is_title(self, paragraph):
        return paragraph.Style.NameLocal.startswith("Heading")

    def __word2text(self, file_path):
        """
        convert word (doc / docx) file into python string
        :param file_path: path to word file
        :return: lines, list of str, containing each line in the doc converted to str.
                to get full text in one string do '\n'.join(lines)
        """
        # print("old f path:",file_path)
        # print("working dir:", os.getcwd())
        file_path = os.path.join(os.getcwd(), file_path.replace('/', '\\'))
        # print("new f path:",file_path)

        doc = self.word.Documents.Open(file_path, ReadOnly=True)

        # todo: change the code
        full_text = []
        for p in doc.Paragraphs:
            tmp_txt = p.Range.Text.strip()
            # if self.__is_title(p):
            #     tmp_txt = f"TT{tmp_txt}TT"
            if self.__is_paragraph_centered(p):
                if self.__is_bold(p):
                    tmp_txt = f"BB{tmp_txt}BB"
                if self.__is_underlined(p):
                    tmp_txt = f"UU{tmp_txt}UU"

                tmp_txt = f'**{tmp_txt}**'
            elif self.__is_underlined(p): # we check this only for MP speach
                tmp_txt = f"UU{tmp_txt}UU"

            if self.__filter_word_texts(tmp_txt):
                full_text.append(tmp_txt)
        lines = [t for t in full_text if len(t.strip()) > 0]
        doc.Close()
        # print(lines)
        return lines



    def __filter_word_texts(self, t):
        if (nlph.rep_title.search(t) is not None) and (nlph.rep_new_debate.search(t) is None): # any centered text that is not adebate title(not center underline bold)
            return False
        if (nlph.rep_new_debate.search(t) is not None) and (nlph.rep_bill_call.search(t.strip()) is not None):
            return False

        return True


    def __parse_IL_plenum(self, lines):
        """
        given lines of plenum doc file, extract all the debates speeches it contains
        :param lines: list of str lines (splitted by '\n')
        :return: dict of all debates from this files, format: [{debate_title: title, speeches: {speaker:speech, ...}, ...]
        """
        lines = '\n'.join(lines)
        # with open("curr_test666.txt", "a+") as f:
        #     f.write(lines)
        #     return
        matches = [match for match in nlph.rep_new_debate.finditer(lines)]
        plenum_started = False

        all_debates = []
        # curr_debate = defaultdict(dict)

        for i in range(len(matches)):
            start, end = matches[i].start(), matches[i].end()
            match = lines[start:end].strip()

            if plenum_started:

                if nlph.rep_title.search(
                        match):  # i know that this is always True but if the code works dont touch it XD

                    # check if match is new debate or table of contents or seder yom
                    if match.strip().strip("**").strip("UU").strip("BB") in nlph.re_bullshit_titles:
                        continue
                    else:  # if it is new debate
                        # get current debate title
                        curr_title = match.strip("**").strip("UU").strip("BB")
                        # if nlph.rep_first_two_bills.search(curr_title):
                        #     curr_title = lines[matches[i-1].start():matches[i-1].end()].strip()

                        if i != len(matches) - 1:
                            curr_debate_speeches = self.__get_IL_debate_speeches(lines, matches[i], matches[i + 1])
                        else: # last debate title in the docx file
                            curr_debate_speeches = self.__get_IL_debate_speeches(lines, matches[i], 0, last=True)

                        if len(curr_debate_speeches) < 2:
                            continue

                        curr_debate = {
                            "debate_title": curr_title.strip("**"),
                            "speeches": curr_debate_speeches
                        }

                        all_debates.append(curr_debate)
                else:
                    pass

                    # split it by '\n' and get speech of each member
            else:

                if nlph.rep_plenum_start.search(match):  # arrived to plenum intro title
                    print("plenum matched:", match)
                    plenum_started = True

        return all_debates


    def __get_IL_debate_speeches(self, all_text, idxs0, idxs1, last=False):
        """
        given indices of current and next debate, extract speeches of the current debate
        using regex
        :param all_text: all plenum file text
        :param idxs0: regex match index of curr debate
        :param idxs1: regex match index of next debate
        :return: dict of speeches, format {speaker: speech}
        """
        # print("getting speeches")
        if not last:
            lines = all_text[idxs0.end():idxs1.start()].strip()
        else:
            lines = all_text[idxs0.end()::].strip()
        all_speeches = defaultdict(str)
        all_speeches = []

        matches = [s for s in nlph.rep_is_speaker.finditer(lines)]

        for i in range(len(matches)):
            start, end = matches[i].start(), matches[i].end()
            speaker = lines[start:end].strip().strip('U')

            if i != len(matches) - 1:
                speech = lines[end:matches[i + 1].start()].strip()
            else:
                speech = lines[end:len(lines)].strip()

            if len(speech) > 5:
                # speeches[speaker] += f"<{speech_idx}>{speech}"

                curr_speech = {
                    "name": speaker,
                    "speech": speech
                }
                all_speeches.append(curr_speech)

        return all_speeches



    def load_pkl(self, file_path):
        with open(file_path, 'rb') as file:
            data = pickle.load(file)

        return data

    def __extract_uk_speaker_name(self, speaker):
        # rep_strings = re.compile(r"{\w\s}+")
        # reg_speaker = rep_strings.findall(speaker)
        if speaker in ["The Prime Minister", "Mr Speaker"]:
            return speaker

        doc = nlp(speaker)
        full_name = ""
        for ent in doc.ents:
            # print(ent, ent.label_)
            if ent.label_ == 'PERSON':
                full_name = ent.text
                break  # Assuming there is only one person entity in the text

        return full_name if full_name != "" else speaker


    def __TN_get_speeches2014(self, lines):
        """
        take text that conttains the full debate and return the speeches as list of dicts
        using regex.
        :param lines: text that includes the full debate with additional symbols like: BB, **.
        :return: list of dicts of format [{"name":name(str), "speech":speech(str)}]
        """
        person = re.compile("BB.+BB")
        centered = re.compile(r"\*\*.+\*\*")

        matches = [match for match in person.finditer(lines)] # include indices of MPs names
        all_speeches = [] # return later, format: [{"name":name(str), "speech":speech(str)}]

        # print(lines)
        for i in range(len(matches)):
            start, end = matches[i].start(), matches[i].end()
            match = lines[start:end].strip()

            match = match.strip("BB")
            if centered.search(match):
                continue

            if i < len(matches) -1:
                # print(len(lines), end, len(matches), i+1)
                speech = lines[end:matches[i + 1].start()].strip()
            else:
                speech = lines[end:len(lines)].strip()

            all_speeches.append(
                {
                    "name": match,
                    "speech": speech
                }
            )

        return all_speeches

    def __TN_get_speeches2019(self, data):
        """
        Function for 2019+ debates, get list of tuples that ordered according to
        the actual debate, and return it as the speeches format
        :param data: list of tuples of format [(name, party, speech)]
        :return: list of dicts of format [{"name":name(str), "speech":speech(str)}]
        """


        # return [{"name":name, "speech":speech} for name, party, speech in data]
        if len(data) == 0:
            return []
        res = []
        for bulk in data:
            if not bulk:
                continue
            try:
                res.extend([{"name": name, "speech": speech} for name, party, speech in bulk])
            except ValueError:
                continue
            # res.append({"name": name, "speech": speech})


            return res

    def __parse_IL_TOR_plenum(self, lines):
        lines = '\n'.join(lines)

        matches = [match for match in nlph.rep_new_debate.finditer(lines)]
        all_debates = []

        for i in range(len(matches)):
            start, end = matches[i].start(), matches[i].end()
            match = lines[start:end].strip()

            if nlph.rep_title.search(
                    match):  # i know that this is always True but if the code works dont touch it XD

                # check if match is new debate or table of contents or seder yom
                strippe_matchd = match.strip().strip("**").strip("UU").strip("BB")
                if strippe_matchd in nlph.re_bullshit_titles or strippe_matchd.startswith("הישיבה ה"):
                    continue
                else:  # if it is new debate
                    # get current debate title
                    curr_title = match.strip("**").strip("UU").strip("BB")
                    # if nlph.rep_first_two_bills.search(curr_title):
                    #     curr_title = lines[matches[i-1].start():matches[i-1].end()].strip()

                    if i != len(matches) - 1:
                        curr_debate_speeches = self.__get_IL_debate_speeches(lines, matches[i], matches[i + 1])
                    else: # last debate title in the docx file
                        curr_debate_speeches = self.__get_IL_debate_speeches(lines, matches[i], 0, last=True)

                    if len(curr_debate_speeches) < 2:
                        continue

                    curr_debate = {
                        "debate_title": curr_title.strip("**"),
                        "speeches": curr_debate_speeches
                    }

                    all_debates.append(curr_debate)
            else:
                continue

        return all_debates


    def UK_debate_members(self, debates_list):
        speeches_dir_path = Data.speeches_files_dir + "/UK"
        name2id = {}
        non_mp_members = set()

        # for csv_path in os.listdir(Data.csv_files_dir):
        #     df = pd.read_csv(csv_path)
        #     for row in df.itterrows():

        idx = 0 # used to save unique non_mp_members file name
        for file_path, debate_date in debates_list:
            self.members = pd.read_csv("Data/csv_files/members/UK_members_backup.csv")
            self.members['startDate'] = pd.to_datetime(self.members['startDate'])
            self.members['endDate'] = pd.to_datetime(self.members['endDate'])

            date = pd.to_datetime(debate_date)

            filtered_df = self.members[((self.members['startDate'] <= date) & (self.members['endDate'] >= date)) | ((self.members['startDate'] <= date) & (self.members['endDate'].isna()))]

            # TOOD: get period

            # debate_date = row["date"]
            # file_path = row["file_path"]
            print("\n\nfile path", file_path)
            unique_names = set()

            if not file_path.endswith(".json"):
                print("non json file detected by Abo Swaid:", file_path)
                continue

            speeches = Data.load_json(file_path)
            for speech in speeches:
                name = self.clean_UK_mp_name(speech["name"])

                if name is None:
                    speech["id"] = -1
                    non_mp_members.add(name)
                    continue

                name = self.__UK_get_real_name(name, unique_names)
                unique_names.add(name)
                # print(name)

                name_id = name2id.get((name, debate_date), self.__UK_get_name_id(name, debate_date, filtered_df))

                speech["id"] = name_id
                if name_id == -1:
                    non_mp_members.add(name)


            # overwrite the og json file path
            Data.save_json(file_path, speeches)

        # TODO: save the non_MP_members into json/pkl and then add it to uk_members.csv
        Data.save_pkl(non_mp_members, f"non_MP_members{idx}_{str(datetime.now()).replace(':', '-')}")
        idx += 1

        print("unique names:", unique_names)

    def clean_UK_mp_name(self, name):

        if name in ["Mr Speaker", "The Chair"]:
            return None

        contain_non_ascii = lambda s: len(s.encode('ascii', errors='ignore')) != len(s)

        ascii_name = []
        # clear non ascii chars
        for w in name.split():
            if not contain_non_ascii(w):
                ascii_name += [w]

        ascii_name = " ".join(ascii_name)
        # rep_mr = r'^\s*(MR\.?|MRS\.?|Dr\.?)\s+'

        rep_name = re.compile(r'\s*\(?((?:MR\.?|MRS\.?|MS\.?|Dr\.?)\s+)?((?:\w|-|\s)+)\)?', re.IGNORECASE)

        return rep_name.search(ascii_name).group(2)

    def __UK_get_real_name(self, name, names):
        if len(names) == 0 or name in names:
            return name.strip()

        name = name.split(" ")

        candidates = defaultdict(lambda: 0)

        # if len(name) == 1:
        for unique_name in names:
            for name_part in name:
                if name_part in unique_name.split():
                    candidates[unique_name] += 1

        if len(candidates) == 0:
            return " ".join(name).strip()

        return max(candidates, key=candidates.get).strip()


    def __UK_get_name_id(self, name, date, filtered_df):
        # if self.members is None:
        #     self.members = pd.read_csv("Data/csv_files/members/UK_members_backup.csv")
        #     self.members['startDate'] = pd.to_datetime(self.members['startDate'])
        #     self.members['endDate'] = pd.to_datetime(self.members['endDate'])
        #
        # date = pd.to_datetime(date)
        #
        # # filter by date
        # filtered_df =  self.members[( self.members['startDate'] <= date) & ( self.members['endDate'] >= date)]
        # print(filtered_df)
        filter_by_name = filtered_df[filtered_df["name"] == name]
        if len(filter_by_name) >= 1:
            return filter_by_name["member_id"].values[0]

        most_similar_name = process.extractOne(name, filtered_df['name'])
        print("sim", most_similar_name)
        if most_similar_name[1] < 90:
            return -1
        most_similar_row = filtered_df[filtered_df['name'] == most_similar_name[0]]

        most_similar_id = most_similar_row["member_id"].values[0] if len(most_similar_row) >= 1 else -1

        return most_similar_id


if __name__ == "__main__":

    # fp1 = r"C:\Users\ayals\Downloads\Prime Minister 2024-03-13.txt"
    # fp2 = r"C:\Users\ayals\Downloads\Point of Order 2024-03-13.txt"
    x = Debates_DataProcessor(10)

    tester = [("Data/UK/usiness ofW2024-06-12 20-19-22.099870.json", "27/02/2020")]
    x.UK_debate_members(tester)
