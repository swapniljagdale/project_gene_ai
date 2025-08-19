import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from base_normalized_processor import BaseNormalizedProcessor

class ExpensesNormalizedProcessor(BaseNormalizedProcessor):
    """
    Processor for normalizing daily expenses data.
    It reads landing expenses data, joins it with normalized events data
    to ensure referential integrity, and writes the result to the normalized layer.
    """
    def __init__(self, spark):
        super().__init__(spark)

    def process(self):
        """
        Executes the normalization process for expenses.
        """
        try:
            # Read landing data for expenses from 'lnd_events_expenses_daily'
            self.logger.info("Reading landing data for expenses")
            expenses_df = self.read_landing_data("lnd_events_expenses_daily")

            # Read normalized data for events to link expenses
            self.logger.info("Reading normalized events data")
            events_df = self.read_normalized_data("lm_events")

            # Join expenses with events on event_vendor_id
            # This ensures that we only process expenses for events that exist in the system
            self.logger.info("Joining expenses with normalized events")
            normalized_expenses_df = expenses_df.join(
                events_df.select("event_vendor_id", "event_id"),
                on="event_vendor_id",
                how="inner"  # Use inner join to only keep expenses for existing events
            )

            # Select and arrange final columns
            final_df = normalized_expenses_df.select(
                col("event_id"),
                col("event_vendor_id"),
                col("event_expense"),
                col("event_expense_estimate"),
                col("event_expense_actual")
            )
            
            # Write the normalized data to 'lm_events_expenses'
            self.logger.info("Writing normalized expenses data")
            self.write_normalized_data(final_df, "lm_events_expenses")

            self.logger.info("Successfully processed and normalized expenses data.")

        except Exception as e:
            self.logger.error(f"Error during expenses normalization process: {str(e)}")
            raise

def main():
    """Main entry point for the script."""
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("ExpensesNormalizedProcessor") \
            .getOrCreate()
        
        processor = ExpensesNormalizedProcessor(spark)
        processor.process()
        
    except Exception as e:
        print(f"Error in ExpensesNormalizedProcessor script: {str(e)}", file=sys.stderr)
        if spark:
            spark.stop()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
