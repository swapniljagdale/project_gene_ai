import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, split, explode, when, trim

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class BaseNormalizedProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.landing_path = "s3://dev-lmhc-datasource/landing/"
        self.normalized_path = "s3://dev-lmhc-datasource/common/normalized/"
        self.master_data_path = "s3://dev-lmhc-datasource/common/master/"
        
    def read_landing_data(self, table_name):
        """Read data from landing layer."""
        try:
            path = f"{self.landing_path}{table_name}"
            logger.info(f"Reading data from landing: {path}")
            return self.spark.read.parquet(path)
        except Exception as e:
            logger.error(f"Error reading landing data: {str(e)}")
            raise
            
    def read_master_data(self, file_name, schema=None):
        """Read master data from CSV files."""
        try:
            path = f"{self.master_data_path}{file_name}"
            logger.info(f"Reading master data: {path}")
            return self.spark.read.option("header", "true").csv(path)
        except Exception as e:
            logger.error(f"Error reading master data {file_name}: {str(e)}")
            raise
            
    def write_normalized_data(self, df, table_name):
        """Write data to normalized layer."""
        try:
            path = f"{self.normalized_path}{table_name}"
            logger.info(f"Writing data to normalized: {path}")
            df.write.mode("overwrite").parquet(path)
            logger.info(f"Successfully wrote {df.count()} records to {path}")
        except Exception as e:
            logger.error(f"Error writing normalized data: {str(e)}")
            raise
            
    def process(self):
        """Main processing method to be implemented by child classes."""
        raise NotImplementedError("Subclasses must implement process method")
