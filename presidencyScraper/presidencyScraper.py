"""A tool for scraping www.presidency.ucsb.edu."""

__version__ = '2.1'

from datetime import datetime
from collections import defaultdict
import json
import logging
from pathlib import Path
import time
from zipfile import ZipFile

from bs4 import BeautifulSoup
import pandas as pd
import requests
from tqdm import tqdm



class PresidencyScraper():
    """A class for web scraping speeches from www.presidency.ucsb.edu."""


    def __init__(self, initialURL:str, timeout:float=1.0, logLevel:int=20, override:bool=True):
        self.initialURL = initialURL
        self.timeout = timeout

        self.baseURL = 'https://www.presidency.ucsb.edu'
        self.unknownID = 'unknown'
        self.documents: dict[str, dict] = {}

        self._checkInitialURL()
        self.directories = self._getDirectories(override)
        
        self.logger = self._setLogger(logLevel)
        self.logger.info(f'{self.__class__.__qualname__} initialized')
        return None


    def _checkInitialURL(self, url:str=None) -> None:
        if url is None:
            url = self.initialURL
        
        if not url.startswith(self.baseURL):
            raise ValueError(f'The provided url does not match the base url of {self.baseURL}')
        
        return None

    @staticmethod
    def _setLogger(logLevel:int) -> logging.Logger:
        logger = logging.getLogger('scraperLogger')
        logger.setLevel(logLevel)

        # Check if handler exists, add if not
        if not logger.hasHandlers():
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logLevel)
            logger.addHandler(console_handler)

        return logger
    
    @staticmethod
    def _getDirectories(override:bool) -> dict[str, Path]:
        if override:
            root = 'PresidencyScraperResult'
        else:
            root = 'PresidencyScraperResult' + datetime.now().strftime("%Y-%m-$d_%H-%M-%S")


        rootDir = Path(__file__).parent / root
        rootDir.mkdir(exist_ok=True)

        directory = {'root': rootDir,
                     'links': rootDir / 'links.txt',
                     'content': rootDir / 'content.json',
                     'metadataCSV': rootDir / 'metadata.csv',
                     'metadataExcel': rootDir / 'metadata.xlsx',
                     'csv': rootDir / 'search_results.csv',
                     'scrapedWebsites': rootDir / 'scrapedWebsites.txt',
                     'zip': rootDir / 'documents.zip'}
        
        directory['scrapedWebsites'].touch()
        directory['content'].touch()

        return directory


    def scrapeContent(self, initialURL:str=None, limit:int=None) -> None:
        
        i = 0
        self.scrapeCounter = 0
        self.pageNr = 1
        url = initialURL if initialURL else self.initialURL
        self._checkInitialURL(self, url)
        self.documents.clear()
        st = time.time()

        with open(self.directories['scrapedWebsites'], 'r') as file:
            scrapedLinks = {link.strip('\n') for link in file.readlines() if not link.strip() == '\n'}


        try:
            # check for already scraped websites and add scraped website to the file.
            with open(self.directories['scrapedWebsites'], 'a') as file:

                while url:
                    self.logger.info(f"Loading results from {url}")

                    page = requests.get(url)
                    soup = BeautifulSoup(page.text, "html.parser")

                    for linkBlock in tqdm(soup.find_all("tr", {"class": ["even", "odd"]}), ncols=80, desc=f'Scraping Page {self.pageNr:03d}'):
                        link = self.baseURL + linkBlock.find("td", {"class": "views-field-title"}).find("a")["href"]
                        self.logger.debug(f"Found link: {link}")
                        
                        if not link in scrapedLinks:
                            self._scrapePage(link)  # scrape the data into self.documents dictionary
                            file.write(f'{link}\n')
                            scrapedLinks.add(link)
                            self.scrapeCounter += 1

                        i += 1
                        if limit is not None and i >= limit:
                            url = None
                            break
                    
                    if url:
                        url = self._getNextPage(soup)
                        time.sleep(self.timeout)


        except Exception as e:
            self.logger.error(e)
        
        finally:
            self.saveJson()
            self._logEndMsg(st)
                    
        return None


    def _logEndMsg(self, st:float) -> None:

        totalSeconds = time.time() - st
        hours, remainingSeconds = divmod(totalSeconds, 60*60)
        minutes, seconds = divmod(remainingSeconds, 60)
        self.logger.warning(f'Scraped {self.scrapeCounter} documents in {hours:02.0f}:{minutes:02.0f}:{seconds:02.0f} hours.')

        return None


    def saveJson(self) -> None:
        
        try:
            with open(self.directories['content'], "r") as file:
                previousDocuments = json.load(file)
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            previousDocuments = {}

        newDocuments = {key: value for key, value in self.documents.items() if key not in previousDocuments}

        if self.documents:
            # Update previousDocuments with new entries and write back to file
            previousDocuments.update(newDocuments)
            with open(self.directories['content'], "w") as outfile:
                json.dump(previousDocuments, outfile)
        
        return None


    def _getNextPage(self, soup:BeautifulSoup) -> str|None:
        
        searchResultPageLink = soup.find("a", {"title": "Go to next page"})

        if searchResultPageLink is None:
            return None
        else:
            self.pageNr += 1
            return self.baseURL + searchResultPageLink["href"]


    def _scrapePage(self, link:str) -> None:
        page = requests.get(link)
        soup = BeautifulSoup(page.text, "html.parser")

        self.documents[link] = self._findPageElements(soup)

        time.sleep(self.timeout)
        return None


    def _findPageElements(self, soup:BeautifulSoup) -> dict: 
        text = soup.find("div", {"class": "field-docs-content"}).text
        date = soup.find("span", {"class": "date-display-single"}).text
        title = self._formatString(soup.find("div", {"class": "field-ds-doc-title"}).text)
        speaker = soup.find("h3", {"class": "diet-title"}).text
        citation = soup.find("p", {"class": "ucsbapp_citation"}).text
        stateElem = soup.find("div", {"class": "field-spot-state"})
        if stateElem is not None:
            state = self._formatString(stateElem.text)
        else:
            state = self.unknownID
        
        city = self._cityFromTitle(title, state)

        categoryContainer = soup.find("div", {"class": "menu-block-wrapper menu-block-7 menu-name-menu-doc-cat-menu parent-mlid-0 menu-level-1"})
        categories = [element.get("title") for element in categoryContainer.find_all(class_="dropdown-toggle") if element.get("title")]

        elements = {"text": text,
                    "date": date,
                    "title": title,
                    "speaker": speaker,
                    "citation": citation,
                    "state": state,
                    "city": city,
                    "categories": categories}
        return elements

    @staticmethod
    def _formatString(string:str) -> str:
        string = string.strip('\n')
        string = string.strip()

        return string


    def _cityFromTitle(self, title:str, state:str) -> str:
        if state and state != self.unknownID:
            if title.endswith(state):
                address = title.split(' in ')[-1]
                city = address.split(',')[0].strip()
                return city
        
        return self.unknownID


    def resultToDataframe(self) -> None:
        """The method creates a excel and csv file for the metadata found in the json file."""
        self.logger.debug('Starting resultToDataframe method...')

        contentAgg = defaultdict(list)

        with open(self.directories['content'], 'r') as file:
            content = json.load(file)
        
        for link, textDict in tqdm(content.items(), ncols=80, desc='Creating df'):
            contentAgg['link'].append(link)

            for key, info in textDict.items():
                if key == 'text':
                    continue

                contentAgg[key].append(info)
        

        df = pd.DataFrame.from_dict(contentAgg)
        df = df[['speaker', 'date', 'state', 'city', 'title', 'citation', 'categories', 'link']]

        df.to_csv(self.directories['metadataCSV'])
        df.to_excel(self.directories['metadataExcel'])

        self.logger.debug(f'Resulting df columns: {df.columns.tolist()}')
        self.logger.info(f'Resulting df shape: {df.shape}, values: {df.size}')
        return None


    def resultToText(self) -> None:

        linkSources = []

        with open(self.directories['content'], 'r') as file:
            content = json.load(file)

        with ZipFile(self.directories['zip'], 'w') as myzip:

            for i, (link, textDict) in tqdm(enumerate(content.items(), 1), ncols=80, desc='Creating txt files'):

                myzip.writestr(f'speech{i:04d}.txt', textDict['text'])
                linkSources.append(f'{i}, {link}')

            linkSourceText = '\n'.join(linkSources)
            myzip.writestr(f'sources.csv', linkSourceText)
        
        return None





if __name__ == '__main__':
    
    url = "https://www.presidency.ucsb.edu/advanced-search?field-keywords=&field-keywords2=&field-keywords3=&from%5Bdate%5D=&to%5Bdate%5D=&person2=200320&category2%5B0%5D=51&category2%5B1%5D=10&items_per_page=100"


    scraper = PresidencyScraper(url, timeout=2.1)
    
    scraper.scrapeContent(limit=600)
    scraper.resultToDataframe()
    scraper.resultToText()




