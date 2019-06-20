#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
from zipfile import ZipFile, BadZipFile, is_zipfile
import unidecode
import json
import multiprocessing


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ImgwCurrentCollector:
    def get(self, locations):
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        result = pool.map(self.process, locations)
        return result

    def process(self, location):
        '''Returns a list of dictionaries containing weather data from danepubliczne.imgw.pl'''

        stripped_location = unidecode.unidecode(location.lower())
        url = f"https://danepubliczne.imgw.pl/api/data/synop/station/{stripped_location}"
        response = requests.get(url)

        if response.status_code == requests.codes.ok:
            logger.info(f"Getting current data for {location}...")
            json_response = response.json()
            return json_response
        else:
            logger.error("URL not responding.")
            raise ConnectionError()


class ImgwHistoricalCollector:
    def __init__(self, years=[2019]):
        self.years = years
        self.renamed = {
            "SZCZECIN": "Szczecin",
            "KRAKÓW-BALICE": "Kraków",
            "WROCŁAW-STRACHOWICE": "Wrocław",
            "WARSZAWA-OKĘCIE": "Warszawa",
            "LUBLIN-RADAWIEC": "Lublin",
            "POZNAŃ-ŁAWICA": "Poznań",
            "WARSZAWA": "Warszawa",
            "POZNAŃ": "Poznań",
            "WROCŁAW": "Wrocław"
        }

    def check_url(self, url):
        response = requests.get(url)
        if response.status_code == requests.codes.ok:
            return response
        else:
            raise ConnectionError('URL not responding.')

    def prepare_urls(self):
        locations = {
            "Szczecin": "205",
            "Wrocław": "424",
            "Warszawa": "375",
            "Kraków": "566",
            "Lublin": "495",
            "Poznań": "330",
        }

        base_url = "https://dane.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne/terminowe/synop/"
        ext = "zip"
        files = []

        for year in self.years:
            url = base_url + str(year)
            page = self.check_url(url).text
            soup = BeautifulSoup(page, 'html.parser')
            if year == 2019:
                files.append([url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)])
            else:
                for node in soup.find_all('a'):
                    file = node.get('href')
                    if file.endswith(ext) and locations[self.location] in file:
                        files.append([url + '/' + node.get('href')])

        return [x for sublist in files for x in sublist]

    def get(self, locations):
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        result = pool.map(self.process, locations)
        return result

    def process(self, location):
        self.location = location
        logger.info(f"Getting historical data for {location}...")
        urls = self.prepare_urls()
        for url in urls:
            content = self.check_url(url)

            data_csv = self.read_zip(content)

            if data_csv:
                dataframe = self.read_csv(data_csv)
                prepared_data = self.prepare_data(dataframe).to_dict(orient="records")
                return prepared_data

    def read_csv(self, csv_file):
        if csv_file:
            try:
                data = pd.read_csv(csv_file.open(csv_file.namelist()[0]), 
                        header=None, encoding="iso8859_2", low_memory=False,
                        parse_dates={'datetime':[2, 3, 4, 5]}, 
                        usecols=[1, 2, 3, 4, 5, 29, 25, 41, 37, 48])
                logger.info("Loaded data from file.")
            except AttributeError as e:
                logger.error(str(e))
                return None

            return data

    def read_zip(self, content):
        try:
            zip_file = ZipFile(BytesIO(content.content))
            logger.info("Extracted zipfile.")
        except BadZipFile as e:
            logger.error(str(e))
            return None
        
        return zip_file

    def prepare_data(self, data_frame):
        data_frame[1] = data_frame[1].replace(self.renamed)
        data_filtered = data_frame.loc[data_frame[1] == self.location]

        return data_filtered


if __name__ == "__main__":
    collector = ImgwHistoricalCollector(years=[2019])
    print(collector.get(["Szczecin"]))