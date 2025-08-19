import sys
from pyspark.sql import SparkSession
from base_landing_processor import BaseLandingProcessor

class ExpensesLandingProcessor(BaseLandingProcessor):
    """Processor for loading daily expenses data into the landing layer."""
    def __init__(self, spark, metadata_df):
        super().__init__(spark, metadata_df, "events_expenses_daily")
        self.key_columns = ["event_vendor_id"]  # Unique key for expenses
        self.non_null_columns = ["event_vendor_id"] # Must have an event identifier

def main():
    """Main entry point for the script."""
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("ExpensesLandingProcessor") \
            .getOrCreate()
        
        # The main metadata file path is passed as an argument
        metadata_path = sys.argv[1]
        metadata_df = spark.read.option("header", "true").csv(metadata_path)

        processor = ExpensesLandingProcessor(spark, metadata_df)
        processor.process()
        
    except Exception as e:
        print(f"Error processing expenses landing data: {str(e)}", file=sys.stderr)
        if spark:
            spark.stop()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
