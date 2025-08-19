import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when, to_date, current_timestamp, current_date
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

class BaseLandingProcessor:
    def __init__(self, spark, metadata_df, object_name):
        self.spark = spark
        self.metadata_df = metadata_df
        self.object_name = object_name
        self.error_bucket = "s3://dev-lmhc-datasource/error/"
        self.landing_snapshot_path = "s3://dev-lmhc-datasource/landing_snapshot/"
        self.landing_path = "s3://dev-lmhc-datasource/landing/"
        self.current_timestamp = current_timestamp()
        self.current_date = current_date()
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

    def get_metadata(self):
        """Get metadata for the current object."""
        try:
            metadata = self.metadata_df.filter(
                col("target_table_name").ilike(f"%{self.object_name}%")
            ).collect()
            
            if not metadata:
                raise ValueError(f"No metadata found for object: {self.object_name}")
                
            return metadata[0]
        except Exception as e:
            self.logger.error(f"Error getting metadata: {str(e)}")
            raise

    def read_preprocess_data(self, preprocess_path):
        """Read data from preprocess location."""
        try:
            self.logger.info(f"Reading preprocess data from: {preprocess_path}")
            df = self.spark.read.parquet(preprocess_path)
            
            # Add metadata columns
            df = df.withColumn("oasis_create_date", self.current_timestamp) \
                  .withColumn("oasis_modified_date", self.current_timestamp)
                  
            return df
        except Exception as e:
            self.logger.error(f"Error reading preprocess data: {str(e)}")
            raise

    def read_snapshot_data(self):
        """Read data from snapshot location."""
        try:
            snapshot_path = f"{self.landing_snapshot_path}{self.object_name}"
            self.logger.info(f"Reading snapshot data from: {snapshot_path}")
            
            # Check if snapshot exists
            try:
                df = self.spark.read.parquet(snapshot_path)
                return df
            except Exception:
                self.logger.warning(f"No snapshot found at {snapshot_path}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error reading snapshot data: {str(e)}")
            raise

    def validate_duplicates(self, df, key_columns):
        """Validate for duplicate records based on key columns."""
        try:
            if not key_columns:
                return df, None
                
            window = Window.partitionBy(key_columns).orderBy(col("oasis_modified_date").desc())
            df_with_dupe_flag = df.withColumn("is_duplicate", F.count("*").over(window) > 1)
            
            dupes_df = df_with_dupe_flag.filter(col("is_duplicate") == True)
            clean_df = df_with_dupe_flag.filter(col("is_duplicate") == False).drop("is_duplicate")
            
            return clean_df, dupes_df
            
        except Exception as e:
            self.logger.error(f"Error validating duplicates: {str(e)}")
            raise

    def validate_nulls(self, df, non_null_columns):
        """Validate for null values in key columns."""
        try:
            if not non_null_columns:
                return df, None
                
            null_condition = None
            for col_name in non_null_columns:
                if null_condition is None:
                    null_condition = col(col_name).isNull()
                else:
                    null_condition = null_condition | col(col_name).isNull()
            
            nulls_df = df.filter(null_condition)
            clean_df = df.filter(~null_condition)
            
            return clean_df, nulls_df
            
        except Exception as e:
            self.logger.error(f"Error validating nulls: {str(e)}")
            raise

    def write_to_landing(self, df, table_name):
        """Write data to landing location."""
        try:
            landing_path = f"{self.landing_path}{table_name}"
            self.logger.info(f"Writing data to landing: {landing_path}")
            
            # Add load time
            df = df.withColumn("oasis_load_time", self.current_date)
            
            # Write to landing
            df.write.mode("overwrite").parquet(landing_path)
            
            self.logger.info(f"Successfully wrote {df.count()} records to {landing_path}")
            
        except Exception as e:
            self.logger.error(f"Error writing to landing: {str(e)}")
            raise

    def write_to_error(self, df, error_reason):
        """Write error records to error location."""
        try:
            if df is None or df.rdd.isEmpty():
                return
                
            error_path = f"{self.error_bucket}{self.object_name}_error/"
            
            # Add error metadata
            error_df = df.withColumn("error_reason", lit(error_reason)) \
                        .withColumn("error_timestamp", self.current_timestamp)
            
            # Write to error location
            error_df.write.mode("append").parquet(error_path)
            
            self.logger.info(f"Wrote {error_df.count()} error records to {error_path}")
            
        except Exception as e:
            self.logger.error(f"Error writing to error location: {str(e)}")
            raise

    def process(self):
        """Main processing method to be implemented by child classes."""
        raise NotImplementedError("Subclasses must implement process method")
