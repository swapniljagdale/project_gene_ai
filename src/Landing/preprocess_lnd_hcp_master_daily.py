import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from base_landing_processor import BaseLandingProcessor

class HcpMasterLandingProcessor(BaseLandingProcessor):
    def __init__(self, spark, metadata_df):
        super().__init__(spark, metadata_df, "hcp_master")
        self.key_columns = ["hcp_id"]  # Replace with actual key columns
        self.non_null_columns = ["hcp_id", "hcp_name"]  # Replace with actual non-null columns
        
    def validate_integrity(self, df, address_df, child_attr_df):
        """Validate referential integrity with address and child_attributes."""
        try:
            # Check for HCPs in master but not in address
            hcp_in_master_not_in_address = df.join(
                address_df.select("hcp_id"),
                on="hcp_id",
                how="left_anti"
            )
            
            # Check for HCPs in master but not in child attributes
            hcp_in_master_not_in_child = df.join(
                child_attr_df.select("hcp_id"),
                on="hcp_id",
                how="left_anti"
            )
            
            # Combine all integrity issues
            integrity_issues = hcp_in_master_not_in_address.union(hcp_in_master_not_in_child).distinct()
            
            # Get clean records
            clean_df = df.join(
                integrity_issues.select("hcp_id"),
                on="hcp_id",
                how="left_anti"
            )
            
            return clean_df, integrity_issues
            
        except Exception as e:
            self.logger.error(f"Error in integrity validation: {str(e)}")
            raise
    
    def process(self):
        try:
            # Get metadata
            metadata = self.get_metadata()
            preprocess_path = f"{metadata['preprocess_location']}/{metadata['target_table_name']}"
            
            # Read preprocess and snapshot data
            preprocess_df = self.read_preprocess_data(preprocess_path)
            snapshot_df = self.read_snapshot_data()
            
            # Read address and child attributes data for integrity check
            address_df = self.spark.read.parquet(f"{self.landing_path}lm_address")
            child_attr_df = self.spark.read.parquet(f"{self.landing_path}lm_child_attributes")
            
            # Union with snapshot if exists
            if snapshot_df:
                # Exclude records that exist in snapshot with same key
                preprocess_df = preprocess_df.join(
                    snapshot_df.select(self.key_columns),
                    on=self.key_columns,
                    how="left_anti"
                )
                
                # Union with snapshot
                final_df = preprocess_df.unionByName(snapshot_df, allowMissingColumns=True)
            else:
                final_df = preprocess_df
            
            # Validate duplicates
            clean_df, dupes_df = self.validate_duplicates(final_df, self.key_columns)
            if dupes_df and not dupes_df.rdd.isEmpty():
                self.write_to_error(dupes_df, "Duplicate key violation")
            
            # Validate nulls
            clean_df, nulls_df = self.validate_nulls(clean_df, self.non_null_columns)
            if nulls_df and not nulls_df.rdd.isEmpty():
                self.write_to_error(nulls_df, "Null value in key column")
            
            # Validate referential integrity
            clean_df, integrity_issues = self.validate_integrity(clean_df, address_df, child_attr_df)
            if integrity_issues and not integrity_issues.rdd.isEmpty():
                self.write_to_error(integrity_issues, "Referential integrity issue - missing in address or child_attributes")
            
            # Write to landing
            self.write_to_landing(clean_df, metadata['target_table_name'])
            
            # Log success
            self.logger.info(f"Successfully processed {self.object_name} data")
            
        except Exception as e:
            self.logger.error(f"Error processing {self.object_name}: {str(e)}")
            raise

def main():
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("HCP_Master_Landing_Processor") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()
        
        # Read metadata (passed as argument or use default)
        metadata_path = sys.argv[1] if len(sys.argv) > 1 else "s3://cmg-oasis-prod-lm-healthcare-datasource/common/metadata/lm_metadata.csv"
        metadata_df = spark.read.option("header", "true").csv(metadata_path)
        
        # Process HCP master
        processor = HcpMasterLandingProcessor(spark, metadata_df)
        processor.process()
        
    except Exception as e:
        print(f"Script failed: {str(e)}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
