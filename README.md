# webscraping-ssi-mef

A web scraping project built with Scrapy to extract and structure data from the Investment Tracking System (SSI, by its Spanish acronym) of Peru’s Ministry of Economy and Finance. The project automates data collection for analysis, monitoring, and visualization of public investment projects.

[![Project Status: Active – The project has reached a stable, usable actively developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

---

## Features

- **Automated Data Extraction**: Efficiently crawls the MEF SSI portal to gather investment data. [Access the Investment Tracking System here](https://ofi5.mef.gob.pe/ssi/).
- **Structured Data**: Transforms unstructured web content into clean, actionable formats (JSON, CSV, etc.).
- **High Performance**: Built on Scrapy's asynchronous engine for fast and scalable data collection.
- **Domain Specific**: Specifically tailored to handle the navigation and data structure of the Investment Tracking System.

## Technologies Used

*   **Language:** Python 3.x
*   **Framework:** [Scrapy](https://scrapy.org/)
*   **Data Handling:** Scrapy Items and Pipelines for clean data processing.

## Project Structure
```text
├── 📁 data/
│   ├── 📁 input/                  # .csv file with investments codes
│   ├── 📁 raw/                    # Cached .json files downloaded from the SSI portal
│   └── 📁 processed/              # Cleaned .csv datasets ready for analysis
├── 📁 webscraping/
│   ├── 📁 spiders/                # Scrapy spiders (data extraction logic)
│   ├── 📄 __init__.py             # Package initializer
│   ├── 📄 items.py                # Data structure definitions (Scrapy Items)
│   ├── 📄 middlewares.py          # Custom request/response middlewares
│   ├── 📄 pipelines.py            # Data cleaning and storage logic
│   └── 📄 settings.py             # Scrapy project settings
├── 📄 main.py                     # Entry point: Orchestrates scraping + ETL flow
├── 📄 scrapy.cfg                  # Scrapy configuration file
├── 📄 requirements.txt            # Python dependencies for the project
├── 📄 README.md                   # Project documentation and usage instructions
├── 📄 LICENSE                     # License information for distribution and usage
├── 📄 .gitignore                  # Files and folders to be ignored by Git
```

## Getting Started

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/edisonlmg/webscraping-ssi-mef.git
    cd webscraping-ssi-mef
    ```

2.  **Create and activate a virtual environment (Recommended):**
    ```bash
    python -m venv venv
    # On Windows:
    .\venv\Scripts\activate
    # On macOS/Linux:
    source venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install scrapy
    ```

## Usage

To start the data extraction, execute the following command:

```bash
scrapy crawl ssi_spider.py
```

## Contributing

Contributions are welcome! Please feel free to open issues or submit pull requests to improve the scraper.

## License

This project is licensed under the Apache License 2.0. See the LICENSE file for details.
