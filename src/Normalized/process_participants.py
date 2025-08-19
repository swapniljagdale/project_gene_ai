import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from base_normalized_processor import BaseNormalizedProcessor

class ParticipantNormalizedProcessor(BaseNormalizedProcessor):
    def __init__(self, spark):
        super().__init__(spark)
        
    def process(self):
        try:
            # Read landing data
            logger.info("Reading participants data from landing layer")
            participants_df = self.read_landing_data("lm_participants")
            
            # Read master data
            logger.info("Reading master xref and mdm merge data")
            master_xref = self.read_master_data("master_xref_participant.csv")
            master_mdm_merge = self.read_master_data("master_mdm_merge.csv")
            
            # Step 1: Join with master_xref to get mdm_id
            logger.info("Joining with master xref to get mdm_id")
            participants_with_mdm = participants_df.join(
                master_xref.select(
                    col("source_id").alias("participant_id"),
                    col("mdm_id")
                ),
                on="participant_id",
                how="left"
            )
            
            # Step 2: Join with master_mdm_merge to handle merged MDM records
            logger.info("Handling merged MDM records")
            participants_with_winner_mdm = participants_with_mdm.join(
                master_mdm_merge.select(
                    col("loser_mdm_id").alias("mdm_id"),
                    col("winner_mdm_id")
                ),
                on="mdm_id",
                how="left"
            )
            
            # Step 3: Determine final mdm_id (use winner_mdm_id if available, otherwise use original mdm_id)
            logger.info("Determining final mdm_id")
            final_participants = participants_with_winner_mdm.withColumn(
                "final_mdm_id",
                when(col("winner_mdm_id").isNotNull(), col("winner_mdm_id"))
                .otherwise(col("mdm_id"))
            )
            
            # Select and order columns for final output
            logger.info("Preparing final participants output")
            output_columns = [
                "participant_id", "event_id", "hcp_id", "participation_status",
                "mdm_id", "final_mdm_id", "oasis_create_date", 
                "oasis_modified_date", "oasis_load_time"
            ]
            
            # Include only columns that exist in the DataFrame
            final_output = final_participants.select(
                [c for c in output_columns if c in final_participants.columns]
            )
            
            # Write to normalized layer
            logger.info("Writing participants to normalized layer")
            self.write_normalized_data(final_output, "lm_participants")
            
            logger.info("Successfully processed participants data")
            
        except Exception as e:
            logger.error(f"Error processing participants: {str(e)}")
            raise

def main():
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("Participant_Normalized_Processor") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()
        
        # Process participants
        processor = ParticipantNormalizedProcessor(spark)
        processor.process()
        
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
