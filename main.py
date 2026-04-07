# main.py

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

if __name__ == "__main__":
    # Load settings from settings.py
    settings = get_project_settings()
    
    # Start the Scrapy process
    process = CrawlerProcess(settings)
    
    # Call the spider by its name ('ssi')
    process.crawl('ssi')
    
    print("Starting SSI data extraction. Please wait...")
    process.start() 
    print("Extraction finished! Check the data/raw and data/processed folders.")
    
