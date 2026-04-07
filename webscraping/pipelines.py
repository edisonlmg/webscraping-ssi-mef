# webscraping_ssi_mef/pipelines.py

import pandas as pd
import os
import json
import logging


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


    def close_spider(self):
        """Saves all files when the Spider finishes and cleans null values"""
        logging.info("Saving raw JSON and processed CSV files (cleaning null values)...")
        
        for key in self.raw_data_collection.keys():
            # --- Save RAW JSON ---
            raw_filepath = os.path.join(self.path_data_raw, f'{key}_raw.json')
            with open(raw_filepath, 'w', encoding='utf-8') as f:
                json.dump(self.raw_data_collection[key], f, ensure_ascii=False, indent=4)
            
            # --- Save UNIFIED CSV ---
            if key in self.datasets and len(self.datasets[key]) > 0:
                df = pd.DataFrame(self.datasets[key])
                
                # Drop columns where ABSOLUTELY ALL values are null
                df = df.dropna(axis=1, how='all')
                
                # Separate our control columns from the API columns
                control_cols = ['investment_code', 'download_status']
                data_cols = [c for c in df.columns if c not in control_cols]
                
                if data_cols:
                    # Keep rows if they have an error (traceability) OR if they have at least one valid data point
                    valid_rows = (df.get('download_status') != 'OK') | (df[data_cols].notna().any(axis=1))
                    df = df[valid_rows]

                # Ensure 'investment_code' and 'download_status' are at the beginning
                final_cols = [c for c in control_cols if c in df.columns] + \
                               [c for c in df.columns if c not in control_cols]
                df = df[final_cols]
                
                # Save the final CSV
                csv_filepath = os.path.join(self.path_data_processed, f'{key}_unified.csv')
                df.to_csv(csv_filepath, encoding='utf-8-sig', index=False)
                
                len_cui = df['investment_code'].nunique()
                logging.info(f'Ok ✅ {key} saved clean. Investments processed: {len_cui}')


