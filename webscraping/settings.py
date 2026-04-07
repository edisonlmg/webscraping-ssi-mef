# webscraping_ssi_mef/settings.py
 
BOT_NAME = "webscraping_ssi_mef"

SPIDER_MODULES = ["webscraping.spiders"]
NEWSPIDER_MODULE = "webscraping.spiders"

# Disable robots.txt rules
ROBOTSTXT_OBEY = False

# Avoid overloading the server 
CONCURRENT_REQUESTS = 4
COOKIES_ENABLED = False

# Smart delay to prevent bans
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 30.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0

# Enable the pipeline
ITEM_PIPELINES = {
   "webscraping.pipelines.SsiProcessingPipeline": 300,
}

# Asynchronous compatibility settings
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

LOG_LEVEL = 'INFO'

