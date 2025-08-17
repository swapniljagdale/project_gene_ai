import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, split, explode, trim, when
from base_normalized_processor import BaseNormalizedProcessor

class EventNormalizedProcessor(BaseNormalizedProcessor):
    def __init__(self, spark):
        super().__init__(spark)
        self.default_brand_id = 412
        self.default_brand_name = "UNKNOWN"
        
    def process(self):
        try:
            # Read landing data
            logger.info("Reading events data from landing layer")
            events_df = self.read_landing_data("lm_events")
            
            # Read master data
            logger.info("Reading product and indication master data")
            product_master = self.read_master_data("product_master.csv")
            indication_master = self.read_master_data("indication_master.csv")
            
            # Step 1: Explode the event_brand column
            logger.info("Splitting and exploding event_brand column")
            events_df = events_df.withColumn(
                "event_brand_single",
                explode(split(col("event_brand"), "\\|"))
            ).withColumn("event_brand", trim(col("event_brand")))
            
            # Step 2: Join with product master on the single brand
            logger.info("Joining with product master on single brand")
            events_with_brand = events_df.join(
                product_master.select(
                    col("source_product_name").alias("event_brand_single"),
                    col("brand_id"),
                    col("brand_name")
                ),
                on="event_brand_single",
                how="left"
            )
            
            # Step 3: Handle unknown brands
            logger.info("Handling unknown brands")
            events_with_brand = events_with_brand.withColumn(
                "brand_id",
                when(col("brand_id").isNull(), lit(self.default_brand_id))
                .otherwise(col("brand_id"))
            ).withColumn(
                "brand_name",
                when(col("brand_name").isNull(), lit(self.default_brand_name))
                .otherwise(col("brand_name"))
            )
            
            # Step 4: Aggregate the results back together
            logger.info("Aggregating brand information back into delimited strings")
            # Group by all original columns except the ones we are aggregating
            grouping_cols = [c for c in events_df.columns if c != 'event_brand_single']

            events_aggregated = events_with_brand.groupBy(*grouping_cols).agg(
                concat_ws("|", collect_list(col("brand_id"))).alias("brand_id"),
                concat_ws("|", collect_list(col("brand_name"))).alias("brand_name"),
                concat_ws("|", collect_list(col("event_brand_single"))).alias("event_brand_merged")
            )

            # Step 5: Join with indication master
            logger.info("Joining with indication master")
            final_events = events_aggregated.join(
                indication_master.select(
                    col("source_indication_name").alias("indication_name"),
                    col("indication_id")
                ),
                on="indication_name",
                how="left"
            )
            
            # Select and order columns for final output
            logger.info("Preparing final events output")
            final_events = final_events.select(
                "event_id",
                "event_name",
                "event_date",
                "event_type",
                "event_vendor_id",
                col("event_brand_merged").alias("event_brand"),
                "brand_id",
                "brand_name",
                "indication_name",
                "indication_id",
                "source_system",
                "source_file_name",
                "source_file_date",
                "oasis_modified_date"
            ).distinct()
            
            output_columns = [
                "event_id", "event_name", "event_date", "event_brand",
                "brand_id", "brand_name", "indication_name", "indication_id",
                "event_location", "oasis_create_date", "oasis_modified_date", "oasis_load_time"
            ]
            
            final_events = final_events.select([c for c in output_columns if c in final_events.columns])
            
            # Write to normalized layer
            logger.info("Writing events to normalized layer")
            self.write_normalized_data(final_events, "lm_events")
            
            logger.info("Successfully processed events data")
            
        except Exception as e:
            logger.error(f"Error processing events: {str(e)}")
            raise

def main():
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("Event_Normalized_Processor") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()
        
        # Process events
        processor = EventNormalizedProcessor(spark)
        processor.process()
        
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
