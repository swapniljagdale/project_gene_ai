import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum as sum_, avg, when, date_format, expr, lit
from datetime import datetime
from base_summarized_processor import BaseSummarizedProcessor

class EventPerformanceKPIs(BaseSummarizedProcessor):
    """Calculate event performance KPIs."""
    
    def process(self):
        try:
            # Read normalized data
            events_df = self.read_normalized_data("lm_events")
            participants_df = self.read_normalized_data("lm_participants")
            
            # Ensure required columns exist
            if "event_date" not in events_df.columns or "participant_id" not in participants_df.columns:
                raise ValueError("Required columns not found in the input data")
            
            # Join events and participants
            event_participants = events_df.join(
                participants_df,
                events_df["event_id"] == participants_df["event_id"],
                "left"
            )
            
            # Calculate KPIs
            kpi_df = event_participants.groupBy(date_format("event_date", "yyyy-MM-dd").alias("event_date")).agg(
                countDistinct("event_id").alias("total_events"),
                count("participant_id").alias("total_registrations"),
                sum_(when(col("attended") == "Y", 1).otherwise(0)).alias("total_attendees"),
                avg(when(col("attended") == "Y", 1).otherwise(0)).alias("avg_attendance_per_event"),
                (sum_(when(col("attended") == "Y", 1).otherwise(0)) / 
                 count("participant_id")).alias("attendance_rate")
            ).withColumn("processing_date", lit(datetime.now().strftime("%Y-%m-%d")))
            
            # Write results
            self.write_summarized_data(kpi_df, "event_performance_kpi")
            logger.info("Successfully generated event performance KPIs")
            
        except Exception as e:
            logger.error(f"Error generating event performance KPIs: {str(e)}")
            raise

class ProductPerformanceKPIs(BaseSummarizedProcessor):
    """Calculate product performance KPIs."""
    
    def process(self):
        try:
            # Read normalized data
            events_df = self.read_normalized_data("lm_events")
            participants_df = self.read_normalized_data("lm_participants")
            
            # Join events and participants
            event_participants = events_df.join(
                participants_df,
                events_df["event_id"] == participants_df["event_id"],
                "left"
            )
            
            # Calculate KPIs by product
            kpi_df = event_participants.groupBy("brand_id", "brand_name").agg(
                countDistinct("event_id").alias("event_count"),
                count("participant_id").alias("total_registrations"),
                sum_(when(col("attended") == "Y", 1).otherwise(0)).alias("total_attendees"),
                (sum_(when(col("attended") == "Y", 1).otherwise(0)) / 
                 countDistinct("event_id")).alias("avg_attendance_per_event")
            ).withColumn("processing_date", lit(datetime.now().strftime("%Y-%m-%d")))
            
            # Write results
            self.write_summarized_data(kpi_df, "product_performance_kpi")
            logger.info("Successfully generated product performance KPIs")
            
        except Exception as e:
            logger.error(f"Error generating product performance KPIs: {str(e)}")
            raise

class ParticipantEngagementKPIs(BaseSummarizedProcessor):
    """Calculate participant engagement KPIs."""
    
    def process(self):
        try:
            # Read normalized data
            participants_df = self.read_normalized_data("lm_participants")
            
            # Ensure required columns exist
            if "participant_type" not in participants_df.columns or "attended" not in participants_df.columns:
                raise ValueError("Required columns not found in participants data")
            
            # Calculate KPIs by participant type
            kpi_df = participants_df.groupBy("participant_type").agg(
                count("participant_id").alias("total_participants"),
                sum_(when(col("attended") == "Y", 1).otherwise(0)).alias("attended_count"),
                (sum_(when(col("attended") == "Y", 1).otherwise(0)) / 
                 count("participant_id")).alias("attendance_rate"),
                (sum_(when((col("registered") == "Y") & (col("attended") == "N"), 1).otherwise(0)) /
                 sum_(when(col("registered") == "Y", 1).otherwise(0))).alias("no_show_rate")
            ).withColumn("processing_date", lit(datetime.now().strftime("%Y-%m-%d")))
            
            # Write results
            self.write_summarized_data(kpi_df, "participant_engagement_kpi")
            logger.info("Successfully generated participant engagement KPIs")
            
        except Exception as e:
            logger.error(f"Error generating participant engagement KPIs: {str(e)}")
            raise

class EventTypeKPIs(BaseSummarizedProcessor):
    """Calculate event type performance KPIs."""
    
    def process(self):
        try:
            # Read normalized data
            events_df = self.read_normalized_data("lm_events")
            participants_df = self.read_normalized_data("lm_participants")
            
            # Ensure required columns exist
            if "event_type" not in events_df.columns:
                raise ValueError("event_type column not found in events data")
            
            # Join events and participants
            event_participants = events_df.join(
                participants_df,
                events_df["event_id"] == participants_df["event_id"],
                "left"
            )
            
            # Calculate KPIs by event type
            kpi_df = event_participants.groupBy("event_type").agg(
                countDistinct("event_id").alias("event_count"),
                count("participant_id").alias("total_registrations"),
                sum_(when(col("attended") == "Y", 1).otherwise(0)).alias("total_attendees"),
                (sum_(when(col("attended") == "Y", 1).otherwise(0)) / 
                 count("participant_id")).alias("attendance_rate"),
                (count("participant_id") / 
                 countDistinct("event_id")).alias("avg_registrations_per_event")
            ).withColumn("processing_date", lit(datetime.now().strftime("%Y-%m-%d")))
            
            # Write results
            self.write_summarized_data(kpi_df, "event_type_participants_kpi")
            logger.info("Successfully generated event type KPIs")
            
        except Exception as e:
            logger.error(f"Error generating event type KPIs: {str(e)}")
            raise

class BrandPerformanceKPIs(BaseSummarizedProcessor):
    """Calculate brand performance KPIs."""
    
    def process(self):
        try:
            # Read normalized data
            events_df = self.read_normalized_data("lm_events")
            participants_df = self.read_normalized_data("lm_participants")
            
            # Join events and participants
            event_participants = events_df.join(
                participants_df,
                events_df["event_id"] == participants_df["event_id"],
                "left"
            )
            
            # Calculate KPIs by brand
            kpi_df = event_participants.groupBy("brand_id", "brand_name").agg(
                countDistinct("event_id").alias("event_count"),
                count("participant_id").alias("total_registrations"),
                sum_(when(col("attended") == "Y", 1).otherwise(0)).alias("total_attendees"),
                (sum_(when(col("attended") == "Y", 1).otherwise(0)) / 
                 count("participant_id")).alias("attendance_rate"),
                (sum_(when(col("attended") == "Y", 1).otherwise(0)) / 
                 countDistinct("event_id")).alias("avg_attendance_per_event")
            ).withColumn("processing_date", lit(datetime.now().strftime("%Y-%m-%d")))
            
            # Write results
            self.write_summarized_data(kpi_df, "brand_performance_kpi")
            logger.info("Successfully generated brand performance KPIs")
            
        except Exception as e:
            logger.error(f"Error generating brand performance KPIs: {str(e)}")
            raise

def main():
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("KPI_Generator") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        # Generate all KPIs
        kpi_generators = [
            EventPerformanceKPIs(spark),
            ProductPerformanceKPIs(spark),
            ParticipantEngagementKPIs(spark),
            EventTypeKPIs(spark),
            BrandPerformanceKPIs(spark)
        ]
        
        for generator in kpi_generators:
            generator.process()
            
        logger.info("All KPI generation completed successfully")
        
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
