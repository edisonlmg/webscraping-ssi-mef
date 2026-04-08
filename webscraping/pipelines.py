# webscraping_ssi_mef/pipelines.py

import os
import json
import logging
import pandas as pd
import re

class SsiProcessingPipeline:
    
    def __init__(self):
        self.datasets = {}
        self.raw_data_collection = {}
        
        # Destination folders
        self.path_data_raw = os.path.join('data', 'raw')
        self.path_data_processed = os.path.join('data', 'processed')
        
        os.makedirs(self.path_data_raw, exist_ok=True)
        os.makedirs(self.path_data_processed, exist_ok=True)

    def process_item(self, item):
        # Extract data from the container
        key = item['endpoint_key']
        cui = item['cui']
        raw_data = item['raw_data']
        
        # Store the raw JSON (RAW)
        if key not in self.raw_data_collection:
            self.raw_data_collection[key] = {}
            
        self.raw_data_collection[key][cui] = raw_data

        # Prepare unified data for the CSV (PROCESSED)
        if key not in self.datasets:
            self.datasets[key] = []
            
        if isinstance(raw_data, list):
            for row in raw_data:
                processed_row = row.copy() if isinstance(row, dict) else {'original_value': row}
                processed_row['investment_code'] = cui
                self.datasets[key].append(processed_row)
                
        elif isinstance(raw_data, dict):
            processed_row = raw_data.copy()
            processed_row['investment_code'] = cui
            self.datasets[key].append(processed_row)

        return item

    @staticmethod
    def convert_custom_date(val):
        """
        Converts the custom date format /Date(1679028490000)/ to a pandas datetime object.
        """
        if isinstance(val, str) and val.startswith('/Date('):
            try:
                # Busca el patrón de forma segura
                match = re.search(r'\((\d+)\)', val)
                
                # Solo extrae el grupo si realmente hubo una coincidencia
                if match:
                    ms_str = match.group(1)
                    # Convert milliseconds to datetime
                    return pd.to_datetime(int(ms_str), unit='ms')
                    
            except (ValueError, TypeError):
                # If conversion fails, return the original value intact
                return val
                
        return val

    def close_spider(self):
        """
        Saves all files when the Spider finishes, parses dates, and cleans null/zero values.
        """
        logging.info("Saving raw JSON and processed CSV files (cleaning data)...")
        
        for key in self.raw_data_collection.keys():
            # --- Save RAW JSON ---
            raw_filepath = os.path.join(self.path_data_raw, f'{key}_raw.json')
            with open(raw_filepath, 'w', encoding='utf-8') as f:
                json.dump(self.raw_data_collection[key], f, ensure_ascii=False, indent=4)
            
            # --- Save UNIFIED CSV ---
            if key in self.datasets and len(self.datasets[key]) > 0:
                df = pd.DataFrame(self.datasets[key])

                # 1. Date Conversion (Targeting columns starting with "FEC", e.g., "FEC_REGISTRO")
                date_cols = [c for c in df.columns if c.upper().startswith('FEC')]
                for col in date_cols:
                    # CORRECCIÓN: Llamar a convert_custom_date usando self
                    df[col] = df[col].apply(self.convert_custom_date)

                # 2. Identify "empty" cells (nulls, numeric 0, or string '0')
                is_empty = df.isna() | (df == 0) | (df == '0')
                
                # 3. Drop columns where ABSOLUTELY ALL values are empty (combinations of nulls/zeros)
                df = df.loc[:, ~is_empty.all(axis=0)]
                
                # Separate control columns from API data columns
                control_cols = ['investment_code', 'download_status']
                data_cols = [c for c in df.columns if c not in control_cols]
                
                if data_cols:
                    # 4. Identify empty cells only within the surviving data columns
                    is_empty_data = df[data_cols].isna() | (df[data_cols] == 0) | (df[data_cols] == '0')
                    
                    # 5. Keep rows if there is a download error (for traceability) 
                    # OR if they have at least one valid (non-empty) data point
                    valid_rows = (df.get('download_status') != 'OK') | (~is_empty_data).any(axis=1)
                    df = df[valid_rows]

                # Reorder columns (ensure control columns are placed at the beginning)
                final_cols = [c for c in control_cols if c in df.columns] + \
                             [c for c in df.columns if c not in control_cols]
                df = df[final_cols]
                
                # Save the final CSV
                csv_filepath = os.path.join(self.path_data_processed, f'{key}_unified.csv')
                df.to_csv(csv_filepath, encoding='utf-8-sig', index=False)
                
                # Logging metrics
                unique_investments_count = df['investment_code'].nunique() if 'investment_code' in df.columns else 0
                logging.info(f'Ok ✅ {key} saved and cleaned. Rows: {len(df)}, Unique investments: {unique_investments_count}')


