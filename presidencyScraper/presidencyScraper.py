""" PresidencyScraper: A web scraper for speeches from www.presidency.ucsb.edu. """

__version__ = '2.3'

# Copyright (C) 2024 Julian Wagner
# This file is part of the PresidencyScraper project.
#
# PresidencyScraper is under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# PresidencyScraper is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with PresidencyScraper. If not, see <http://www.gnu.org/licenses/>.


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
    """A class for web scraping from www.presidency.ucsb.edu. This class provides methods to scrape speeches, save the content in 
    various formats (JSON, TXT, CSV, Excel), and filter the scraped data based on include and exclude criteria. It also logs the 
    scraping process and handles directory and file management for the scraped data.

    Arguments:
        initialURL (str): The initial URL to start scraping from. It is created by adjusting the advanced search found here https://www.presidency.ucsb.edu/advanced-search.
        timeout (float): The timeout between requests to avoid overloading the server. Set with caution.
        logLevel (int): The log level for the logger (default Python values).
        override (bool): A flag to indicate if the output directory should be overridden.
        include (dict): A dictionary specifying the metadata keys and their corresponding values to include in the scraping. -> see metadataKeys
        exclude (dict): A dictionary specifying the metadata keys and their corresponding values to exclude from the scraping. -> see metadataKeys
    
    Class Attributes:
        metadataKeys (list): A list of keys representing the metadata fields to be scraped.
        subStrSuffix (str): A suffix used to identify substring keys in the include and exclude dictionaries.
        unknownID (str): A string to represent missing values in the city and state fields.

    Main Methods:
        scrape(initialURL=None, limit=None): 
            Scrapes the content of the website and stores it as JSON, TXT, and metadata as CSV/Excel.
        scrapeContent(initialURL=None, limit=None):
            The main loop for scraping the contents of the website. Saves the content in a JSON file.
            Tries to extract the city from the title of the speech. If it fails, it returns the unknownID.
        resultToDataframe():
            Creates an Excel and CSV file for the metadata found in the JSON file.
        resultToText():
            Creates a ZIP file with the text of the speeches and a CSV file with the sources.
    
    """
    
    metadataKeys = ['text', 'date', 'title', 'speaker', 'citation', 'state', 'city', 'categories']
    subStrSuffix = '_substring'
    unknownID = 'unknown'


    def __init__(self, initialURL:str, timeout:float=1.0, logLevel:int=20, override:bool=True, include:dict[str, list]={}, exclude:dict[str, list]={}):
        self.initialURL = initialURL
        self.timeout = timeout
        self.include = include
        self.exclude = exclude

        self.baseURL = 'https://www.presidency.ucsb.edu'
        self.documents: dict[str, dict] = {}

        self._checkInitialURL()
        self._checkIncludeExclude()
        self.directories = self._getDirectories(override)
        
        self.logger = self._setLogger(logLevel)
        self.logger.info(f'{self.__class__.__qualname__} initialized')
        return None


    def _checkInitialURL(self, url:str=None) -> None:
        """The method checks if the provided url is reachable and if it points to the correct webpage."""

        if url is None:
            url = self.initialURL
        
        if not url.startswith(self.baseURL):
            raise ValueError(f'The provided url does not match the base url of {self.baseURL}')
        
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an HTTPError for bad responses
        except requests.RequestException as e:
            raise ValueError(f'The provided url is not reachable: {url}. Error: {e}')

        return None


    def _checkIncludeExclude(self) -> None:
        """The method checks if the keys of the include and exclude dictionaries are valid, i.e. they are present in the metadata keys."""

        for scope, name in [(self.include, 'include'), (self.exclude, 'exclude')]:
            
            # checks if all keys are inside self.meatadataKeys
            if not all([key.replace(self.subStrSuffix, '') in self.metadataKeys for key in scope.keys()]):
                invalidKeys = [key for key in scope.keys() if key.replace(self.subStrSuffix, '') not in self.metadataKeys]
                raise ValueError(f'The {name} argument contains invalid keys which are not present in self.metadataKeys: {invalidKeys}')

            # checks if all values are lists
            if not all([isinstance(value, list) for value in scope.values()]):
                invalidValues = [key for key, value in scope.items() if not isinstance(value, list)]
                raise ValueError(f'The {name} argument contains invalid values which are not lists: {invalidValues}')

        return None

    @staticmethod
    def _setLogger(logLevel:int) -> logging.Logger:
        """The method sets up a basic logger for the class."""

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
        """The method creates a dictionary with the paths to the directories and files that will be created during the scraping process."""

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


    def scrape(self, initialURL:str=None, limit:int=None) -> None:
        """The method scrapes the content of the website and stores it as .json as well as txt files and metadata as csv/excel."""

        self.scrapeContent(initialURL, limit)
        self.resultToDataframe()
        self.resultToText()
        return None


    def scrapeContent(self, initialURL:str=None, limit:int=None) -> None:
        """The method scrapes has the main loop for scraping the contents of the website. It saves the content in a json file."""
        
        i = 0
        self.scrapeCounter = 0
        self.pageNr = 1
        url = initialURL if initialURL else self.initialURL
        self._checkInitialURL(url)
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
            self._saveJson()
            self._logEndMsg(st)
                    
        return None


    def _logEndMsg(self, st:float) -> None:
        """The method logs the end message of the scraping process."""

        totalSeconds = time.time() - st
        hours, remainingSeconds = divmod(totalSeconds, 60*60)
        minutes, seconds = divmod(remainingSeconds, 60)
        self.logger.warning(f'Scraped {self.scrapeCounter} documents in {hours:02.0f}:{minutes:02.0f}:{seconds:02.0f} hours.')

        return None


    def _saveJson(self) -> None:
        """The method saves the self.documents dictionary to a json file."""
        
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
        """The method finds the link to the next page of the search results and returns it if there is any."""
        
        searchResultPageLink = soup.find("a", {"title": "Go to next page"})

        if searchResultPageLink is None:
            return None
        else:
            self.pageNr += 1
            return self.baseURL + searchResultPageLink["href"]


    def _scrapePage(self, link:str) -> None:
        """The method scrapes the content of a single page and saves it in the self.documents dictionary."""

        page = requests.get(link)
        soup = BeautifulSoup(page.text, "html.parser")

        elements = self._findPageElements(soup)

        if self._pageIsInSearchScope(elements):
            self.documents[link] = elements
            self.scrapeCounter += 1

        time.sleep(self.timeout)
        return None


    def _pageIsInSearchScope(self, elements:dict[str, str]) -> bool:
        """The method checks if the elements of a page are in the scope of the scraper, i.e. checking with the include and exclude dictionary.."""

        for key, includeValues in self.include.items():
            if key in elements.keys():
                if includeValues and not elements[key] in includeValues:
                    return False
            else:
                elem = elements[key.replace(self.subStrSuffix, '')]
                if not all(value in elem for value in includeValues):
                    return False
 
        for key, excludeValues in self.exclude.items():
            if key in elements.keys():
                if elements[key] in excludeValues:
                    return False
            else:
                elem = elements[key.replace(self.subStrSuffix, '')]
                if any(value in elem for value in excludeValues):
                    return False
            
        return True


    def _findPageElements(self, soup:BeautifulSoup) -> dict:
        """The method finds the wanted elements of a page and returns them in a dictionary."""

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
        categories = ', '.join([element.get("title") for element in categoryContainer.find_all(class_="dropdown-toggle") if element.get("title")])

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
        """The method formats a string by stripping it of leading and trailing whitespaces and newline characters."""

        string = string.strip('\n')
        string = string.strip()

        return string


    def _cityFromTitle(self, title:str, state:str) -> str:
        """The method tries to extract the city from the title of the speech. If it fails, it returns the unknownID."""

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

        populationPath = 'presidencyScraper/USPopulation/SUB-EST2020_ALL.csv'
        self.populationDF = pd.read_csv(populationPath, sep=',')


        with open(self.directories['content'], 'r') as file:
            content = json.load(file)
        
        for link, textDict in tqdm(content.items(), ncols=80, desc='Creating df'):
            contentAgg['link'].append(link)

            for key, info in textDict.items():
                if key == 'text':
                    continue

                contentAgg[key].append(info)
        

        df = pd.DataFrame.from_dict(contentAgg)

        df['population'] = df.apply(lambda row: self._addPopulationCount(self.populationDF, row['state'], row['city']), axis=1)

        df = df[['speaker', 'date', 'state', 'city', 'population', 'title', 'citation', 'categories', 'link']]

        df.to_csv(self.directories['metadataCSV'])
        df.to_excel(self.directories['metadataExcel'])

        self.logger.debug(f'Resulting df columns: {df.columns.tolist()}')
        self.logger.info(f'Resulting df shape: {df.shape}, values: {df.size}')
        return None


    def resultToText(self) -> None:
        """The method creates a zip file with the text of the speeches and a csv file with the sources."""

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

    @staticmethod
    def _addPopulationCount(populationDF:pd.DataFrame, state:str, city:str) -> int:
        """The method adds the population count of a city to the dataframe."""

        populationCol = 'CENSUS2010POP'

        # get all rows matching the (beginning of the) city name
        cityDF = populationDF.loc[populationDF['NAME'].str.startswith(city)]

        # get a list of all elements of the population column where the state matches
        result = cityDF.loc[populationDF['STNAME'] == state].get(populationCol).to_list()

        if result:
            population = int(result[-1])
        else:
            population = -1

        return population




if __name__ == '__main__':
    
    url =  "https://www.presidency.ucsb.edu/advanced-search?field-keywords=&field-keywords2=&field-keywords3=&from%5Bdate%5D=01-01-2008&to%5Bdate%5D=10-28-2024&person2=&category2%5B0%5D=63&items_per_page=100&f%5B0%5D=field_docs_attributes%3A205"

    include = {'speaker': ['John McCain', 'Barack Obama', 'Mitt Romney', 'Hillary Clinton', 'Donald J. Trump', 'Joseph R. Biden, Jr.', 'Kamala Harris']}
    exclude = {'title_substring': ['Press Release']}

    scraper = PresidencyScraper(url, timeout=2.1, include=include, exclude=exclude)

    scraper.scrape(limit=8)


