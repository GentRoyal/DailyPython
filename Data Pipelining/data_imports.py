import pandas as pd
from sqlalchemy import create_engine, text
import logging


logger = logging.getLogger('data import')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def create_db_engine(dbpath):
    try:
        engine = create_engine(dbpath)
        with engine.connect() as conn:
            pass
        logger.info("Database Engine Created Successfully")
        return engine        
    except Exception :
        logger.error("SQLAlchemy is needed. Please import..")
    
def read_query(engine, query):
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        logger.info("Database Read Successfully")
        return df
    except Exception as e:
        logger.error(f"Error: {e}")

def read_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        logger.info("File Read Successfully")
        return df
    except Exception:
        logger.error(f"{file_path} does not exist")