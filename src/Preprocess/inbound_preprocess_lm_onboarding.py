import os
import sys
import re
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import boto3
from botocore.exceptions import ClientError
from pyspark.sql.utils import AnalysisException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class LMOnboardingProcessor:
    def __init__(self, spark, metadata_path):
        self.spark = spark
        self.metadata_path = metadata_path
        self.metadata_df = None
        self.error_bucket = "s3://dev-lmhc-datasource/error/"
        self.file_patterns = {
            'event': r'LMHC_EVENT_\d{14}\.csv$',
            'participant': r'LMHC_PARTICIPANT_\d{14}\.csv$',
            'event_del': r'LMHC_EVENT_DEL_\d{14}\.csv$',
            'hcp_master': r'LMHC_hcp_master_file_\d{8}\.csv$',
            'address': r'LMHC_address_file_\d{8}\.csv$',
            'child_attributes': r'LMHC_child_attributes_\d{8}\.csv$'
        }
        self.load_metadata()

    def load_metadata(self):
        """Load and validate the metadata file."""
        try:
            logger.info(f"Loading metadata from {self.metadata_path}")
            self.metadata_df = self.spark.read.option("header", "true").csv(self.metadata_path)
            
            # Validate metadata structure
            required_columns = [
                'inbound_location', 'preprocess_location', 'file_prefix',
                'delimiter', 'expected_column_list', 'target_table_name'
            ]
            
            missing_columns = [col for col in required_columns if col not in self.metadata_df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns in metadata: {', '.join(missing_columns)}")
                
            logger.info("Successfully loaded and validated metadata file.")
            
        except Exception as e:
            logger.error(f"Error loading metadata file: {str(e)}")
            raise

    def validate_file_naming(self, file_path, file_type):
        """Validate file naming convention based on file type."""
        file_name = os.path.basename(file_path)
        pattern = self.file_patterns.get(file_type.lower())
        
        if not pattern:
            return False, f"Unknown file type: {file_type}"
            
        if not re.match(pattern, file_name, re.IGNORECASE):
            return False, f"File {file_name} does not match expected pattern for {file_type}"
            
        return True, "Naming convention valid"

    def move_to_error(self, source_path, error_message):
        """Move file to error location with error message."""
        try:
            if not source_path.startswith('s3://'):
                logger.error(f"Cannot move non-S3 file to error location: {source_path}")
                return False
                
            s3 = boto3.client('s3')
            source_bucket = source_path.split('/')[2]
            source_key = '/'.join(source_path.split('/')[3:])
            
            # Create error path
            filename = source_path.split('/')[-1]
            error_key = f"error/{filename}"
            
            # Copy to error location
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            s3.copy_object(
                Bucket=source_bucket,
                CopySource=copy_source,
                Key=error_key
            )
            
            # Delete source file
            s3.delete_object(Bucket=source_bucket, Key=source_key)
            
            logger.error(f"Moved {source_path} to error location. Error: {error_message}")
            return True
            
        except Exception as e:
            logger.error(f"Error moving file to error location: {str(e)}")
            return False

    def validate_file(self, file_path, expected_columns, expected_delimiter, file_type):
        """Validate file format, headers, and content."""
        try:
            # Validate file naming convention
            is_valid_naming, naming_msg = self.validate_file_naming(file_path, file_type)
            if not is_valid_naming:
                return False, naming_msg
            
            # Check if file exists
            try:
                df = self.spark.read.option("header", "true").option("delimiter", expected_delimiter).csv(file_path)
            except Exception as e:
                return False, f"Error reading file with delimiter '{expected_delimiter}': {str(e)}"
            
            # Check if file is empty
            if df.rdd.isEmpty():
                return False, "File is empty"
            
            # Check if all expected columns exist and are in the correct order
            actual_columns = df.columns
            expected_columns_list = [col.strip() for col in expected_columns.split(',')]
            
            if actual_columns != expected_columns_list:
                return False, f"Column mismatch. Expected: {expected_columns_list}, Found: {actual_columns}"
            
            return True, "Validation passed"
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"

    def process_files(self):
        """Process all files based on metadata."""
        try:
            # Convert metadata to Pandas for easier iteration
            metadata_pd = self.metadata_df.toPandas()
            
            for _, row in metadata_pd.iterrows():
                try:
                    inbound_path = row['inbound_location']
                    file_prefix = row['file_prefix']
                    file_type = row['target_table_name'].lower()
                    
                    logger.info(f"Processing files for {file_type} from {inbound_path}")
                    
                    # Find all files in the inbound location with the given prefix
                    s3 = boto3.client('s3')
                    bucket = inbound_path.split('/')[2]
                    prefix = '/'.join(inbound_path.split('/')[3:])
                    
                    # List all files in the S3 location
                    paginator = s3.get_paginator('list_objects_v2')
                    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
                    
                    files_to_process = []
                    for page in page_iterator:
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                file_key = obj['Key']
                                if file_key.endswith('/'):  # Skip directories
                                    continue
                                    
                                file_path = f"s3://{bucket}/{file_key}"
                                file_name = os.path.basename(file_path)
                                
                                # Check if file matches the expected prefix
                                if file_name.startswith(file_prefix):
                                    files_to_process.append(file_path)
                    
                    if not files_to_process:
                        logger.warning(f"No files found with prefix {file_prefix} in {inbound_path}")
                        continue
                    
                    for file_path in files_to_process:
                        try:
                            logger.info(f"Processing file: {file_path}")
                            
                            # Validate file
                            is_valid, message = self.validate_file(
                                file_path,
                                row['expected_column_list'],
                                row['delimiter'],
                                file_type
                            )
                            
                            if not is_valid:
                                self.move_to_error(file_path, message)
                                continue
                            
                            # Read the file with the correct delimiter
                            df = self.spark.read.option("header", "true") \
                                               .option("delimiter", row['delimiter']) \
                                               .csv(file_path)
                            
                            # Add processing timestamp
                            df = df.withColumn("processing_timestamp", lit(datetime.now()))
                            
                            # Write to preprocess location in parquet format
                            preprocess_path = f"{row['preprocess_location']}/{row['target_table_name']}"
                            df.write.mode("overwrite").parquet(preprocess_path)
                            
                            logger.info(f"Successfully processed {file_path} to {preprocess_path}")
                            
                            # Delete source file after successful processing
                            s3.delete_object(Bucket=bucket, Key=os.path.relpath(file_path, f"s3://{bucket}/"))
                            
                        except Exception as e:
                            error_msg = f"Error processing file {file_path}: {str(e)}"
                            logger.error(error_msg)
                            self.move_to_error(file_path, error_msg)
                
                except Exception as e:
                    logger.error(f"Error in processing metadata row: {str(e)}")
                    continue
            
            logger.info("File processing completed.")
            
        except Exception as e:
            logger.error(f"Error in process_files: {str(e)}")
            raise 1

def main():
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("LM_Onboarding_Preprocess") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()
        
        # Configure S3 access
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID', ''))
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY', ''))
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
        
        # Path to metadata file (can be passed as an argument)
        metadata_path = sys.argv[1] if len(sys.argv) > 1 else "s3://cmg-oasis-prod-lm-healthcare-datasource/common/metadata/lm_metadata.csv"
        
        # Initialize and run processor
        processor = LMOnboardingProcessor(spark, metadata_path)
        processor.process_files()
        
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
