# Presidency Scraper
The PresidencyScraper is a class for web scraping from www.presidency.ucsb.edu. This class provides methods to scrape speeches, save the content in various formats (JSON, TXT, CSV, Excel), and filter the scraped data based on include and exclude criteria. It also logs the scraping process and handles directory and file management for the scraped data.

## 1 Getting started
Change the url variable (and timeout, include/exclude) at the bottom and run the script. The script is thoroughly tested with Python 3.12.

```
if __name__ = '__main__':
    url =  "https://www.presidency.ucsb.edu/advanced-search?field-keywords=&field-keywords2=&field-keywords3=&from%5Bdate%5D=&to%5Bdate%5D=&person2=200300&items_per_page=100"

    scraper = PresidencyScraper(url, timeout=1.5)
    scraper.scrape(limit=20)
```

## 2 Documentation

### 2.1 Arguments
**initialURL (str)**: The initial URL to start scraping from. It is created by adjusting the advanced search found here

**timeout (float)**: The timeout between requests to avoid overloading the server. Set with caution. Default is 1.0

**logLevel (int)**: The log level for the logger (default Python values). Default is 20.

**override (bool)**: A flag to indicate if an already created output directory should be overridden. Default is true.

**include (dict) / exclude (dict)**: Optional arguments that allow you to specify certain values for a category to be included or excluded from the scraped results. The keys must match to the scraped metadata defined in the *metadataKeys* class variable. The values must be of type list[str]. It is possible to add the substring '_substring' to a key. It will then not look for exact matches of the provided values and instead check if the values from the webpage include a substring of the provided value.

### 2.2 Class Attributes
**metadataKeys (list):** A list of keys representing the metadata fields to be scraped.

**subStrSuffix (str):** A suffix used to identify substring keys in the include and exclude dictionaries.

**unknownID (str):** A string to represent missing values in the city and state fields.

### 2.3 Main Methods
**scrape(initialURL=None, limit=None)**: Scrapes the content of the website and stores it as JSON, TXT, and metadata as CSV/Excel.

**scrapeContent(initialURL=None, limit=None)**: The main loop for scraping the contents of the website. Saves the content in a JSON file. Tries to extract the city from the title of the speech. If it fails, it returns the unknownID.

**resultToDataframe()**: Creates an Excel and CSV file for the metadata found in the JSON file.

**resultToText()**: Creates a ZIP file with the text of the speeches and a CSV file with the sources.

## 3 License

This project is licensed under the GNU General Public License v3.0. See the [LICENSE](../LICENSE.txt) file for details.