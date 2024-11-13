import logging
import re
from process_fields import ProcessFields
from data_imports import read_csv


logger = logging.getLogger("WeatherFieldProcessor")
logging.basicConfig(level=logging.INFO, format = '%(asctime)s - %(name) - %(message)')
    
class WeatherFieldProcessor:
    
    def __init__(self, config):
        self.df = None
        self.weather_station_field = config['weather_station_location']
        self.weather_messages = config['weather_messages']
        self.id_field = config['id_field']
        self.patterns = config['patterns']
        
    def read_weather_and_field_data(self):
        # Code to read map data and weather messages (CSV files)
        self.df = read_csv(self.weather_messages)
        return self.df

     
    def extract_weather_measurements(self, message):
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
    
            if match:
                return key, float(next((x for x in match.groups() if x is not None)))
            #logger.info("Weather Measurements Extracted")

        #logger.info("No pattern matched")
        return None, None

    
    def process_all(self):
        self.df = self.read_weather_and_field_data()       
        result = self.df['Message'].apply(self.extract_weather_measurements)
        self.df['Measurement'] = result.apply(lambda x : x[0])
        self.df['Value'] = result.apply(lambda x : x[1])

        logger.info("Weather Data Loaded Successfully!")

        return self.df