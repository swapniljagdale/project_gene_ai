import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as max_
from pyspark.sql.types import TimestampType

# Configure logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class DataExporter:
    def __init__(self, spark):
        self.spark = spark
        self.normalized_path = "s3://dev-lmhc-datasource/common/normalized/"
        self.output_path = "s3://dev-lmhc-datasource/common/outbound/"
        self.tables = ["lm_events", "lm_participants"]
        self.delimiter = "|"
        
    def get_latest_modified_date(self, table_name):
        """Get the latest modified date from the table."""
        try:
            df = self.spark.read.parquet(f"{self.normalized_path}{table_name}")
            
            # Check which modified date column exists (oasis_modified_date or modified_date)
            date_columns = ["oasis_modified_date", "modified_date"]
            modified_date_col = next((col for col in date_columns if col in df.columns), None)
            
            if not modified_date_col:
                logger.warning(f"No modified date column found in {table_name}, exporting all records")
                return None
                
            # Get max modified date
            max_date = df.agg(max_(col(modified_date_col).cast("timestamp"))).collect()[0][0]
            logger.info(f"Latest modified date for {table_name}: {max_date}")
            return max_date
            
        except Exception as e:
            logger.error(f"Error getting latest modified date for {table_name}: {str(e)}")
            return None
    
    def export_table_to_csv(self, table_name, modified_date=None):
        """Export table data to CSV, filtered by modified_date if provided."""
        try:
            # Read the table
            df = self.spark.read.parquet(f"{self.normalized_path}{table_name}")
            
            # Filter by modified_date if provided
            if modified_date is not None:
                # Check which modified date column exists
                date_columns = ["oasis_modified_date", "modified_date"]
                modified_date_col = next((col for col in date_columns if col in df.columns), None)
                
                if modified_date_col:
                    df = df.filter(col(modified_date_col).cast("timestamp") == modified_date)
            
            # Get current date for filename
            current_date = datetime.now().strftime("%Y%m%d")
            output_file = f"OUT_{table_name.upper()}_{current_date}.csv"
            output_path = f"{self.output_path}{output_file}"
            
            # Write to CSV
            logger.info(f"Exporting {df.count()} records to {output_path}")
            df.write \
                .option("header", "true") \
                .option("delimiter", self.delimiter) \
                .mode("overwrite") \
                .csv(output_path)
                
            logger.info(f"Successfully exported {table_name} to {output_path}")
            
        except Exception as e:
            logger.error(f"Error exporting {table_name}: {str(e)}")
            raise
    
    def export_latest_data(self):
        """Export latest data for all tables."""
        try:
            for table in self.tables:
                logger.info(f"Processing table: {table}")
                
                # Get latest modified date
                latest_date = self.get_latest_modified_date(table)
                
                # Export data
                self.export_table_to_csv(table, latest_date)
                
            logger.info("Export process completed successfully")
            
        except Exception as e:
            logger.error(f"Error during export process: {str(e)}")
            raise

def main():
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("Data_Exporter") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        # Export data
        exporter = DataExporter(spark)
        exporter.export_latest_data()
        
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
