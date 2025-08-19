import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from base_landing_processor import BaseLandingProcessor

class ParticipantLandingProcessor(BaseLandingProcessor):
    def __init__(self, spark, metadata_df):
        super().__init__(spark, metadata_df, "participant")
        self.key_columns = ["participant_id", "event_id"]  # Replace with actual key columns
        self.non_null_columns = ["participant_id", "event_id", "hcp_id"]  # Replace with actual non-null columns
        
    def process(self):
        try:
            # Get metadata
            metadata = self.get_metadata()
            preprocess_path = f"{metadata['preprocess_location']}/{metadata['target_table_name']}"
            
            # Read preprocess and snapshot data
            preprocess_df = self.read_preprocess_data(preprocess_path)
            snapshot_df = self.read_snapshot_data()
            
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
            .appName("Participant_Landing_Processor") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()
        
        # Read metadata (passed as argument or use default)
        metadata_path = sys.argv[1] if len(sys.argv) > 1 else "s3://cmg-oasis-prod-lm-healthcare-datasource/common/metadata/lm_metadata.csv"
        metadata_df = spark.read.option("header", "true").csv(metadata_path)
        
        # Process participants
        processor = ParticipantLandingProcessor(spark, metadata_df)
        processor.process()
        
    except Exception as e:
        print(f"Script failed: {str(e)}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
