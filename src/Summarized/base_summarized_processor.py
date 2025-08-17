import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum as sum_, avg, when, date_format, expr

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class BaseSummarizedProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.normalized_path = "s3://dev-lmhc-datasource/common/normalized/"
        self.summarized_path = "s3://dev-lmhc-datasource/summarized/"
        
    def read_normalized_data(self, table_name):
        """Read data from normalized layer."""
        try:
            path = f"{self.normalized_path}{table_name}"
            logger.info(f"Reading data from normalized: {path}")
            return self.spark.read.parquet(path)
        except Exception as e:
            logger.error(f"Error reading normalized data: {str(e)}")
            raise
            
    def write_summarized_data(self, df, table_name):
        """Write data to summarized layer."""
        try:
            path = f"{self.summarized_path}{table_name}"
            logger.info(f"Writing data to summarized: {path}")
            df.write.mode("overwrite").parquet(path)
            logger.info(f"Successfully wrote {df.count()} records to {path}")
        except Exception as e:
            logger.error(f"Error writing summarized data: {str(e)}")
            raise
            
    def process(self):
        """Main processing method to be implemented by child classes."""
        raise NotImplementedError("Subclasses must implement process method")
