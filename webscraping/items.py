# webscraping_ssi_mef/items.py

import scrapy

class SsiFetchItem(scrapy.Item):
    endpoint_key = scrapy.Field()
    cui = scrapy.Field()
    raw_data = scrapy.Field()


