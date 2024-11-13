import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from data_imports import create_db_engine, read_query, read_csv
import logging

logger = logging.getLogger("processing fields")
logging.basicConfig(level=logging.INFO, format = '%(asctime)s - %(name) - %(message)')

class ProcessFields:

    def __init__(self, config):
        self.df = None
        self.dbpath = config['dbpath']
        self.query = config['query']
        self.absolute_column = config['absolute_column']
        self.id_field = config['id_field']
        self.fill_columns = config['fill_columns']
        self.values_to_rename = config['values_to_rename']
        self.columns_to_rename = config['columns_to_rename']
        self.columns_to_subset = config['columns_to_subset']
        self.date_to_fix = config['date_to_fix']
        self.weather_mapping = config['weather_station_location']

    def import_data(self):
        engine = create_db_engine(self.dbpath)
        self.df = read_query(engine, self.query)

        return self.df

    def rename_columns(self):
        logging.info(f"Renaming columns: {self.columns_to_rename}")
        self.df = self.df.rename(columns=self.columns_to_rename)
    
        logging.info(f"Columns {self.columns_to_rename} Renamed")
        return self.df

    def correct_absolute_error(self):
        try:
            self.df[self.absolute_column] = self.df[self.absolute_column].abs()
            logger.info(f"{self.absolute_column} now contains positive values")        
            return self.df
    
        except Exception as e:
            logger.error(f"{self.absolute_column} is not numeric. Error: {e}")

    def populate_id(self):
        try:
            field_ids = []
            
            for i in range(1, len(self.df[self.id_field]) + 1):
                if i < 10:
                    field = 'F00' + str(i)
                elif i < 100:
                    field = 'F0' + str(i)
                else:
                    field = 'F' + str(i)
                field_ids.append(field)
        
            self.df[self.id_field] = field_ids
            logger.info(f'{self.id_field} values are now are in the expected format')
    
            return self.df
        except Exception as e:
            logger.error(f"Error: Column {e} does not exist")
    
    def fill_missing_values(self):
        for col in self.fill_columns:
            logger.info(f"Processing column: {col}")
            
            if col == 'crop_type':
                # Replacing values and filling missing ones
                original_values = self.df[col].isnull().sum()
                self.df[col] = self.df[col].str.title().replace(self.values_to_rename).fillna('Unspecified Crop')
                new_values = self.df[col].isnull().sum()
                logger.info(f"Filled missing values in '{col}'. Null count changed from {original_values} to {new_values}.")

            elif col == 'soil_type':
                # Handling 'soil_type' column
                original_values = self.df[col].isnull().sum()
                self.df[col] = self.df[col].str.title().fillna('Unspecified Soil')
                new_values = self.df[col].isnull().sum()
                logger.info(f"Filled missing values in '{col}'. Null count changed from {original_values} to {new_values}.")

            else:
                # Handling all other columns
                original_values = self.df[col].isnull().sum()
                self.df[col] = self.df[col].str.title().fillna('None')
                new_values = self.df[col].isnull().sum()
                logger.info(f"Filled missing values in '{col}'. Null count changed from {original_values} to {new_values}.")

        return self.df


    def subset_columns(self):
        for col in self.columns_to_subset:
            logger.info(f"Processing column: {col}")
            
            if col == 'temperature_celsius':
                original_values = self.df[col].isnull().sum()
                self.df[col] = self.df[col].apply(lambda x: x if 0 <= x <= 40 else np.nan)
                new_values = self.df[col].isnull().sum()
                logger.info(f"Temperature column '{col}' processed. Null count changed from {original_values} to {new_values}.")

            elif col == 'ph_level':
                original_values = self.df[col].isnull().sum()
                self.df[col] = self.df[col].apply(lambda x: x if 0 <= x <= 14 else np.nan)
                new_values = self.df[col].isnull().sum()
                logger.info(f"PH Level column '{col}' processed. Null count changed from {original_values} to {new_values}.")

            elif col == 'pest_control':
                original_values = self.df[col].isnull().sum()
                self.df[col] = self.df[col].apply(lambda x: re.sub(r'L.*', '', str(x)) if pd.notnull(x) else np.nan).astype(float).fillna(0)
                new_values = self.df[col].isnull().sum()
                logger.info(f"Pest Control column '{col}' processed. Null count changed from {original_values} to {new_values}.")

            else:
                original_values = self.df[col].isnull().sum()
                self.df[col] = self.df[col].apply(lambda x: re.sub(r'k.*', '', str(x)) if pd.notnull(x) else np.nan).astype(float).fillna(0)
                new_values = self.df[col].isnull().sum()
                logger.info(f"Other column '{col}' processed. Null count changed from {original_values} to {new_values}.")
                
        return self.df
        
    def fix_date(self):
        try:
            self.df[self.date_to_fix] = pd.to_datetime(self.df[self.date_to_fix], errors='coerce')
            logger.info("Date conversion successful for column: %s", self.date_to_fix)
            return self.df
            
        except Exception as e:
            logger.error("Failed to convert date for column %s: %s", self.date_to_fix, e)

    def map_weather_to_fields(self):
        # Code to map weather readings to corresponding fields
        weather_mapping = read_csv(self.weather_mapping)
        field_ids = []
            
        for i in weather_mapping['field_id']:
            if i < 10:
                field = 'F00' + str(i)
            elif i < 100:
                field = 'F0' + str(i)
            else:
                field = 'F' + str(i)
            field_ids.append(field)
        
        weather_mapping['field_id'] = field_ids
        
        self.df = self.df.merge(weather_mapping[['field_id', 'weather_station']], on = 'field_id', how = 'left')
        logger.info("Weather station successfully merged with field records. Records merged: %d", len(weather_mapping))

        return self.df

    def process_all(self):
        self.df = self.import_data()
        self.df = self.correct_absolute_error()
        self.df = self.populate_id()
        self.df = self.fill_missing_values()
        self.df = self.subset_columns()
        self.df = self.fix_date()
        self.df = self.map_weather_to_fields()
        self.df = self.rename_columns()

        return self.df