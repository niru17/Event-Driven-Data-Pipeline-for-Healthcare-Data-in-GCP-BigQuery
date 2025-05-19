from pyspark.sql import SparkSession
from  pyspark.sql.functions import col,to_date
import argparse

bq_project="fit-legacy-454720-g4"
bq_dataset="hospital"
bq_table="health_record"
def process_data(date):
    try:
        spark=SparkSession.builder.appName("BigQuery Hospital Data Ingestion").getOrCreate()
        file_path=f"gs://bigquery-projectss/hospital/health_records_{date}.json"

        print("Reading:", file_path)
        df = spark.read.json(file_path)
        df.printSchema()
        print("Columns:", df.columns)
        df.show(truncate=False)
        print("Raw count:", df.count())

        transformed_df = df.filter(col("diagnosis_code").substr(1, 1) == "C")
        print("After startswith(C):", transformed_df.count())

        non_null_df = transformed_df.filter(
            col("patient_id").isNotNull() &
            col("hospital_id").isNotNull() &
            col("diagnosis_code").isNotNull()
        ).withColumn("diagnosis_date", to_date("diagnosis_date", "yyyy-MM-dd"))

        print("After null filter:", non_null_df.count())
        non_null_df.show(5, truncate=False)

        non_null_df.write \
            .format("com.google.cloud.spark.bigquery.v2.Spark33BigQueryTableProvider") \
            .option("table", f"{bq_project}:{bq_dataset}.{bq_table}") \
            .option("writeMethod", "direct") \
            .option("createDisposition", "CREATE_IF_NEEDED") \
            .option("writeDisposition", "WRITE_APPEND") \
            .mode("append") \
            .save()

        print("Data written to BigQuery.")         
    except Exception as e:
        print(f"Error occurred due to: {e}")
        raise

if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Today file date in YYYYMMDD format")
    args=parser.parse_args()

    process_data(date=args.date)