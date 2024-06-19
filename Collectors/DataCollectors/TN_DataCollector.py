import pandas as pd
import re
from Collectors.DataCollectors.DataCollector import DataCollector
import requests as reqs
from bs4 import BeautifulSoup as bs
from collections import defaultdict
from Data.GLOBAL import Data
from time import time
from datetime import datetime, timedelta


class TN_DataCollector(DataCollector):

    url = 'https://majles.marsad.tn/ar/chronicles?periodId=1&page=10&paginationId=0&between=2014-12-02%20-%202019-11-11'


    def __init__(self, batch_size):
        super(TN_DataCollector, self).__init__(batch_size)

    def get_debates(self):
        print("Collector(TN debates) started")

        since = time()

        # get dates from progress.json
        json_prog = Data.get_progress()
        start_date = json_prog['TN_debates_start_date']
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = start_date + timedelta(days=self.batch_size)

        # calculate new parameters for the custom url
        periodId = 1 if start_date < datetime(2020, 10, 1) else 2
        between = f"{start_date.strftime('%Y-%m-%d')}%20-%20{end_date.strftime('%Y-%m-%d')}"

        # get links of the search page of TN website
        url_endpoint = 'https://majles.marsad.tn'
        search_url = url_endpoint + f"/ar/chronicles?between={between}"
        print(search_url)
        links = self.__get_links(url_endpoint, search_url)
        if len(links) == 0:
            print("Collector(TN debates) no debates found")
            json_prog['TN_debates_start_date'] = end_date.strftime("%Y-%m-%d")
            Data.update_progress(json_prog)
            print("Collector(TN debates) finished")
            return
        # print(links)
        print(f"Collector(TN debates) found {len(links)} links")
        before_2019 = periodId == 1
        # print(links)

        all_debates = []
        for link in links[3::]:
            # get debate date
            date = "-".join(link.split("/")[-5:-2])  # format: "dd-mm-yy

            # get debate from link and convert it into string
            if before_2019:
                title, debate_data = self.debate_before_2019(
                    link)  # here debate data is string text that have to be processed using regex
            else:
                title, debate_data = self.debate_after_2019(
                    link)  # here debate data is a list of tuples [(name, party, speech)]

            # print(title)
            if debate_data == "" or debate_data == []:
                continue

            curr_debate = {
                "periodID": periodId,
                'date': date,
                'debate_title': title,
                'data': debate_data
            }

            # save debate in all_debates
            all_debates.append(curr_debate)
            # print("\n\n\n\\n\n\n\n\n\n\n\n\n\n")

        # save this batch into json
        json_file_name = f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json"
        Data.save_json(f"{Data.processor_debates_dir}/TN/{json_file_name}", all_debates)

        json_prog['TN_debates_start_date'] = end_date.strftime("%Y-%m-%d")
        Data.update_progress(json_prog)
        print("total debates:", len(all_debates))
        print("elepsed: ", time() - since)
        print("Collector(TN debates) finished")

    def debate_before_2019(self, link):
        # link = "/ar/event/2020/10/02/09/plenary" # TODO: delete this test
        res = reqs.get('https://majles.marsad.tn' + link)
        soup = bs(res.content, "html.parser")
        body = soup.find("body", recursive=True)

        debate_title = soup.title.string  # return it later

        # there are two section tags in the page, debates are in the second section
        try:
            section2 = body.find_all("section", recursive=True)[1]
        except IndexError:
            return debate_title, ""

        # sometimes there are pages that divides the debate into sub-debates with element of class "block-chronicle_chunk"
        # example of page that contains this element: https://majles.marsad.tn/ar/event/2020/10/02/09/plenary
        # exmample of page that doesnt contain this element: https://majles.marsad.tn/ar/event/2020/07/09/14/conflits-interet
        debates_iterator = section2.find_all(class_="block-chronicle_chunk", recursive=True)
        debates_iterator = debates_iterator if len(debates_iterator) >= 1 else [section2]

        debate_as_str = []
        for debate_section in debates_iterator:
            # find first paragraph element which is the start of the contents

            curr_tag = debate_section.find("p")

            while curr_tag is not None:
                if curr_tag.name == "ul":
                    line = ' '.join(item.get_text() for item in curr_tag.find_all("li"))
                else:
                    line = curr_tag.text

                # get info about the tag like bold / center / etc..
                tag_info = self.get_tag_info(curr_tag)
                # print(tag_info['center'], tag_info['bold'], tag_info['list'], tag_info['bullshit'])
                if tag_info['bullshit']:
                    curr_tag = curr_tag.next_sibling
                    continue

                if tag_info['center']:
                    line = f"**{line}**"

                # enter new line at each new p (good for regex later)
                if tag_info['bold']:
                    line = f"BB{line}BB"

                debate_as_str.append(line)
                curr_tag = curr_tag.next_sibling

        return debate_title, "\n".join(debate_as_str)

    def get_tag_info(self, curr_tag):
        """
        get useful info about a tag, information are represented as flags
        if a flag is True means the info is true, for example info['center'] == True means that the tag is centerd
        extracted flags:
            - list: tag is a list of points (ul element)
            - bullshit: tag contains more than one element (and isnt a list),  it is bullshit because it is not a speech or parliament name
            - center: tag is in the center, which may mean it is a title
            - bold: tag is bolded, which may mean it is a MP name

        :param curr_tag: bs html element
        :return: defaultdict(False), each flag is False by default
        """
        info = defaultdict(lambda: False)  # each flag is False unless we update it

        if curr_tag.find("strong", recursive=True) is not None:
            info['bold'] = True

        for t in [curr_tag] + curr_tag.find_all(recursive=False):
            # print(t)
            # check if p contains u1 (list of points)
            if t.name == "ul":
                info['list'] = True
                return info
            # if curr_p has more than one child (recursevely) then pass
            elif len(t.find_all(recursive=False)) > 1:
                info['bullshit'] = False
                return info

            # check if p is centered
            if t.get("style") is not None and "center" in t.get("style"):
                info['center'] = True

            # check if p contains strong tag,
            if t.find("strong") is not None:
                info['bold'] = True

        return info

        # print(content)

        # split by centered titles

        # for each content within a title:
        # - elements that contain strong are members names, (also contain () )
        # - only elements that each of its sub elements are of size 1 is relevant
        #

    def debate_after_2019(self, link):
        # link="/ar/event/2021/07/15/10/droits-libertes" # normal case
        # # link="/ar/event/2021/07/14/11/droits-libertes" # inner speaker case
        # # link="/ar/event/2021/07/15/09/ri" # includes votes cards
        # link="/ar/event/2021/07/16/09/jeunesse" # only one speech
        print("debate link: ", link)
        res = reqs.get('https://majles.marsad.tn' + link)
        soup = bs(res.content, "html.parser")
        body = soup.find("body", recursive=True)

        all_speeches = []

        debate_title = soup.title.string  # return it later
        try:
            section2 = body.find_all("section", recursive=True)[1]

            debates = section2.find(recursive=False)

            tags = debates.find_all(recursive=False)
        except AttributeError:
            return debate_title, []
        except IndexError:
            return debate_title, []

        # print(len(curr_tag))
        # return
        # while curr_tag is not None:
        for curr_tag in tags[::]:  # TODO: change index
            print("\n\n ################# NEW ONE NIGGA ###############\n\n")
            # print(curr_tag.attrs)
            # print(curr_tag.text)
            # print("\n\n")
            # TODO: filter non speech tags (tags that only contain description or similar)
            speeches = self.__get_speeches_after2019(curr_tag)
            all_speeches.append(speeches)
            # curr_tag = curr_tag.next_sibling
        print("speeches", all_speeches)
        return debate_title, all_speeches

    def __get_speeches_after2019(self, curr_tag):
        # TODO: filter non speech tags
        # print(curr_tag.attrs)
        # debate = curr_tag.find(id="section").contents[1].contents[0].contetns[0].contents[0].contents[0]
        all_speeches = []

        debate = curr_tag.find(class_="col-md-7 order-md-1 order-12", recursive=True)
        if debate is not None:
            print(curr_tag.attrs)
            for child in debate.contents:
                print(child.text)
                if "block-intervention" not in child.get("class"):
                    continue
                names, parties, speeches = self.get_person_speech(child)
                curr_speeches = list(zip(names, parties, speeches))
                all_speeches.extend(curr_speeches)

        return all_speeches

    def get_person_speech(self, tag):
        """
        this function for debates after 2019, check if the tag contains a container of person talking
        :param tag: bs4 html element object
        :return: bool
        """
        # print(tag.get("class"))
        # if "block-intervention" not in tag.get("class"):
        #     return False

        print("is person? ", tag.attrs)
        # check if the tag contains photo and name
        names = []
        parties = []
        speeches = []

        # tag_links = tag.find_all("a")
        tags_iter = tag.find(recursive=False)
        if tags_iter is not None:
            for child in tags_iter.contents:
                if self.is_inner_person(child):
                    tmp_res = self.get_person_speech(child.find(class_="block-intervention", recursive=True))
                    if not tmp_res:
                        continue
                    tmp_names, tmp_parties, tmp_speeches = tmp_res
                    names.extend(tmp_names)
                    parties.extend(tmp_parties)
                    speeches.extend(tmp_speeches)
                    continue

                child_links = [i.get("href") for i in child.find_all("a")]
                if len(child_links) > 0:
                    name, party, skip = self.__get_person_details(child, child_links)
                    # if skip and False:
                    #     print("func is_person skipped a block")
                    #     continue
                    if name is not None:
                        names.append(name)
                    if party is not None:
                        parties.append(party)
                    print(name)
                else:
                    speech = self.__get_person_speech(child)
                    if speech is not None:
                        speeches.append(speech)

                        print(speech)
                        print("\n")

            # print(len(child_links))
        return names, parties, speeches

    def __get_person_details(self, tag, child_links):
        """
        this func is for 2019+ debates. get person name and party name from its HTML element links
        :param child_links: list of strings that contains url links
        :return: (name: str, party:str, skip:bool (true if name or party has values))
        """

        # the following const links is used in the TN website HTML to
        # redirect into some personal web page. we extracted them and use
        # them to detect the party member name
        const_links = {
            "MP name link": "/ar/person/",  # used for person name
            "party link": "/ar/assembly/blocs/"  # used for party name

        }
        name, party, skip = None, None, True
        for link in child_links:
            if const_links["MP name link"] in link:
                # get person name
                name = tag.find("strong")
                if name is None:
                    return None, None, True
                skip = False
                name = name.text

            elif const_links["party link"] in link:
                # get party name
                party = tag.find("small")
                if party is not None:
                    party = party.text

        return name, party, skip

    def __get_links(self, url_endpoint, url):
        """
        This func is used for all debates. Get all the links of the search page of the website. filter the links
        so only the links of debates are returned
        :param url: search url
        :param url_endpoint: website host url str
        :return:
        """
        final_res = []

        while True:
            resp = reqs.get(url)
            soup = bs(resp.content, 'html.parser')
            all_links = soup.find_all('a')
            links = [link.get('href') for link in all_links if link.get('href').startswith('/ar/event/')]

            final_res.extend(links[::2])

            # get url of the next page
            next_button = soup.find(class_="expand-section more")
            if next_button is None:
                break

            url = url_endpoint + next_button.find("a").get("data-load-more")

        return final_res

    def __get_person_speech(self, child):
        """
        this func is for 2019+ debates. get the speech of an MP, speech is the points list
        :param child: bs4 element
        :return: str - the speech
        """
        # print(child.attrs, child.text)
        speech = child.find("ul")

        return speech.text if speech is not None else None

    def is_inner_person(self, child):
        """
        This func is for 2019+ debates. debates contains "cards" of Mp speeches and
        may contain an inner card (for example when an MP is replying to other). this
        func checks if this is the case
        :param child: bs4 element
        :return: bool - weather it is an inner card or not
        """
        return child.find(class_="block-intervention", recursive=True) is not None


    def get_parties(self):
        print("Collector(TN bills) started")
        # url = "https://majles.marsad.tn/ar/assembly/deputies?periodId=3"
        periods_ID = [i for i in range(3,18)]# this is a parameter in the HTTP request
        all_parties = set()
        for periodID in periods_ID:
            print(f"period {periodID}/18")
            url = f"https://majles.marsad.tn/ar/assembly/deputies?periodId={periodID}"
            resp = reqs.get(url)
            soup = bs(resp.content, 'html.parser')

            # TODO: try-except
            parties_tag = soup.find("body").find_all("section")[2].find_all(recursive=False)[1]
            # print(parties_tag.attrs)
            for party_tag in parties_tag.find_all(recursive=False):
                # print(party_tag.attrs)
                party_name = party_tag.find("h5").text
                all_parties |= {party_name.strip()}

        country = Data.country2code["TN"]
        df = pd.DataFrame(list(all_parties), columns=["party_name"])
        df["country"] = country
        df.to_csv(f"{Data.csv_files_dir}/parties/TN_parties.csv")
        return all_parties

    def get_members(self):
        print("Collector(TN members) started")
        periods_ID = [i for i in range(1,3)] # this is a parameter in the HTTP request
        all_members = defaultdict(list)

        for periodID in periods_ID:
            print(f"period {periodID}/2")
            url = f"https://majles.marsad.tn/ar/assembly/deputies?periodId={periodID}"
            resp = reqs.get(url)
            soup = bs(resp.content, 'html.parser')

            # TODO: try-except
            members_tag = soup.find("body").find(class_="cards-container").find(recursive=False)

            members_links = {link.get("href") for link in members_tag.find_all("a") if ((link.get("href").startswith("/ar/person/")) and ("#questions" not in link.get("href")))}
            # print(members_links)
            all_members = self.__get_members_details(all_members, members_links)

        json_file_path = f"{Data.processor_dir}/members/TN/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json"
        Data.save_json(json_file_path, all_members)


    def __get_members_details(self, all_members, members_links):
        url_endpoint = 'https://majles.marsad.tn'

        for link in members_links:
            url = url_endpoint + link
            resp = reqs.get(url)
            soup = bs(resp.content, 'html.parser')

            member_name = soup.find(class_="name").text.strip()
            if all_members.get(member_name, None) is None:
                member_info = soup.find(class_="profile-info-container my-5 mt-md-0")

                party_history = member_info.find("ul")
                for party in party_history.find_all("li"):
                    party_name = party.find_all(recursive=False)[0].text.strip()
                    party_period = party.find_all(recursive=False)[1].text.strip()
                    all_members[member_name].append((party_name, party_period))
        return all_members




    def get_bills(self):
        periods = [1, 2]

        page = 2
        all_bills = []  # list of dicts save later as csv
        for period in periods:
            pageID = 1
            url = f"https://majles.marsad.tn/ar/legislation/?periodId={period}"
            while url is not None:

                print(period, "/ 2")


                resp = reqs.get(url)
                soup = bs(resp.content, 'html.parser')

                bill_cards = soup.find_all(class_="list-card red-marker")

                for bill_card in bill_cards:
                    bill_date = bill_card.find(class_="date col-sm", recursive=True).text.strip()
                    bill_date = self.__AR2date(bill_date)
                    bill_title = self.__get_bill_title(bill_card)


                    curr_bill = {
                        "title": bill_title,
                        "date": bill_date
                    }
                    all_bills.append(curr_bill)

                # get url of next page, None if there is no next page
                url = self.get_next_page_bills(soup)


        csv_file_path = f"{Data.csv_files_dir}/bills/TN_bills.csv"
        print(len(all_bills))
        pd.DataFrame(all_bills).to_csv(csv_file_path)


    def __get_bill_title(self, bill_card):
        for link in bill_card.find_all("a"):
            if link.get("href").startswith("/ar/legislation/") and "vote" not in link.get("href"):
                bill_title = link.text.strip().split("\n")[0]
                return bill_title


    def __AR2date(self, bill_date):
            rep_day = re.compile(r"\d{2}")
            rep_month = re.compile(r"[^\d]+")
            rep_year = re.compile("\d{4}")
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

            year = rep_year.findall(bill_date)[0]
            month = month_ar2num[rep_month.findall(bill_date)[0].strip()]
            day = rep_day.findall(bill_date)[0]

            return f"{year}-{month}-{day}"

    def get_next_page_bills(self, soup):
        url_endpoint = "https://majles.marsad.tn"
        button = soup.find(class_="expand-section more")

        if button is not None:
            url = url_endpoint + button.find("a").get("data-load-more")
            print(url)
            return url


if __name__ == "__main__":
    a = TN_DataCollector(50)
    x = a.debate_before_2019(50)
    print(x[0], "\n\n", x[1])
    # [print(i) for i in x]
    # print(x)
    # print(len(x))
