# webscraping_ssi_mef/spiders/ssi_spider.py

import os
import scrapy
import pandas as pd
from scrapy.http import JsonRequest
from webscraping.items import SsiFetchItem 


class SsiSpider(scrapy.Spider):
    
    name = 'ssi'
    
    # --- GLOBAL VALUES (Hardcoded as per requirement) ---
    
    GLOBAL_HEADERS = {
        'Content-Type': 'application/json; charset=utf-8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36'
    }

    # Master dictionary
    ENDPOINTS = {
        'traeDetInvSSI_SIAF':        {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeDetInvSSI', 'tipo': 'SIAF'},
        'traeListaParalizaPublico':  {'url': 'https://ofi5.mef.gob.pe/invierte/paraliza/traeListaParalizaPublico', 'tipo': None},
        'traeDevengSSI_FINAN':       {'url': 'https://ofi5.mef.gob.pe/invierteWS/Dashboard/traeDevengSSI', 'tipo': 'FINAN'},
        'traeFonafeSSI_FINAN':       {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeFonafeSSI', 'tipo': 'FINAN'},
        'traeDevengSSI_DEV2':        {'url': 'https://ofi5.mef.gob.pe/invierteWS/Dashboard/traeDevengSSI', 'tipo': 'DEV2'},
        'traeDevengSSI_MES':         {'url': 'https://ofi5.mef.gob.pe/invierteWS/Dashboard/traeDevengSSI', 'tipo': 'MES'},
        'traeFonafeSSI_ANIO':        {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeFonafeSSI', 'tipo': 'ANIO'},
        'traeDevEspecifica_ESPECIF': {'url': 'https://ofi5.mef.gob.pe/invierteWS/Dashboard/traeDevEspecifica', 'tipo': 'ESPECIF'},
        'traeDevengSSI_UEP':         {'url': 'https://ofi5.mef.gob.pe/invierteWS/Dashboard/traeDevengSSI', 'tipo': 'UEPM'},
        'traeDevengSSI_FTE':         {'url': 'https://ofi5.mef.gob.pe/invierteWS/Dashboard/traeDevengSSI', 'tipo': 'FTE'},
        'traeDevengSSI_UEPM':        {'url': 'https://ofi5.mef.gob.pe/invierteWS/Dashboard/traeDevengSSI', 'tipo': 'UEPM'},
        'traeDevengSSI_UEPA':        {'url': 'https://ofi5.mef.gob.pe/invierteWS/Dashboard/traeDevengSSI', 'tipo': 'UEPA'},
        'traeCierreBrecha_BRE':      {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeCierreBrecha', 'tipo': 'BRE'},
        'traeListaConvOXI':          {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeListaConvOXI', 'tipo': None},
        'traeFoniprelSSI_FONI':      {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeFoniprelSSI', 'tipo': 'FONI'},
        'traeInfSeguimF12B':         {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeInfSeguimF12B', 'tipo': None},
        'traeDevengPIM_FINAN':       {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeDevengPIM', 'tipo': 'FINAN'},
        'traeDevengPIM_DEV2':        {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeDevengPIM', 'tipo': 'DEV2'},
        'traeProgEjecFinan':         {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeProgEjecFinan', 'tipo': None},
        'traeProgEjecFinanAct':      {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeProgEjecFinanAct', 'tipo': None},
        'traeDevengPIM_MES2':        {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeDevengPIM', 'tipo': 'MES2'},
        'traeProgFinanAnual':        {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeProgFinanAnual', 'tipo': None},
        'traeAdjudicOXI_CIPRL':      {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeAdjudicOXI', 'tipo': 'CIPRL'},
        'traeHistSituPFA_SIT':       {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeHistSituPFA', 'tipo': 'SIT'},
        'traeHistSituPFA_PFA':       {'url': 'https://ofi5.mef.gob.pe/invierteWS/Ssi/traeHistSituPFA', 'tipo': 'PFA'},
    }

    async def start(self):
        """Loads CUIs from the CSV and generates requests using the dictionary values."""
        
        input_path = os.path.join('data', 'input', 'DETALLE_INVERSIONES.csv')
        
        if not os.path.exists(input_path):
            self.logger.error(f"❌ FILE NOT FOUND: {input_path}")
            return

        try:
            df_inv = pd.read_csv(input_path)
            codes = df_inv['CODIGO_UNICO'].astype(str).unique().tolist()
            self.logger.info(f"✅ Loaded {len(codes)} investment codes.")
        except Exception as e:
            self.logger.error(f"❌ Error reading the CSV: {e}")
            return

        # Generate requests reading from the ENDPOINTS dictionary
        for key, config in self.ENDPOINTS.items():
            url = config['url']
            api_type = config['tipo']

            for cui in codes:
                payload = {'id': int(cui)}
                if api_type:
                    payload['tipo'] = api_type

                yield JsonRequest(
                    url=url,
                    data=payload,
                    headers=self.GLOBAL_HEADERS,
                    meta={'key_file': key, 'cui': cui},
                    callback=self.parse,
                    errback=self.errback,
                    dont_filter=True 
                )

    def parse(self, response):
        """Processes the responses and populates the item container."""
        
        key = response.meta['key_file']
        cui = response.meta['cui']
        
        try:
            data_json = response.json()
        except ValueError:
            self.logger.error(f"Error decoding JSON for {cui} in {key}")
            data_json = None

        # Populate the item
        item = SsiFetchItem()
        item['endpoint_key'] = key
        item['cui'] = cui
        item['raw_data'] = data_json
        
        yield item

    def errback(self, failure):
        """Handles connection errors and logs the failed attempt."""
        
        cui = failure.request.meta.get('cui', 'Unknown')
        key = failure.request.meta.get('key_file', 'Unknown')
        
        self.logger.error(f'Request error for CUI {cui} ❌: {repr(failure)}')
        
        # Explicitly record the failure
        item = SsiFetchItem()
        item['endpoint_key'] = key
        item['cui'] = cui
        item['raw_data'] = None
        
        yield item

