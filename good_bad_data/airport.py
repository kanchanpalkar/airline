from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from utils_main import Commonutils
import argparse
import shutil
import os

# Initialization Spark Session
if __name__ == '__main__':
    spark = SparkSession.builder.appName("AirportMgmtSystem").master("local[*]").getOrCreate()

    # Initialize parser using argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("-s", "--source_name", default="airport", help="please provide source name/dataset name")
    parser.add_argument("-sbp", "--schema_base_path",
                        default=r"C:\Users\kagar\pyspark_projects\AIRLINE_FINAL_PROJECT\schema\airport_schema",
                        help="please provide Schema Base Path")
    parser.add_argument("-bs", "--base_path",
                        default=r"C:\Users\kagar\pyspark_projects\AIRLINE_FINAL_PROJECT\raw_files",
                        help="please provide input Base Path")
    parser.add_argument("-l", "--layer", default="Bronze", help="please Provide layer name  Bronze/Silver/Gold name")
    parser.add_argument("-lp", "--loading_base_path", default=r"C:\Users\kagar\pyspark_projects\AIRLINE_FINAL_PROJECT\processed_data",
                        help="please provide loading path ")
    parser.add_argument("-SCD_1", "--Do_u_want_to_apply_scd1", default="True",
                        help="please provide input True or False")

    # Create argument of parser
    args = parser.parse_args()

    # Initialization argument variables
    source_name = args.source_name
    schema_base_path = args.schema_base_path
    base_path = args.base_path
    layer = args.layer
    loading_base_path = args.loading_base_path
    SCD_1 = args.Do_u_want_to_apply_scd1

    # Dates: Today and Yesterday
    today_date1 = (datetime.now() - timedelta(days=2))
    today_date = today_date1.strftime("%Y%m%d")
    yesterday_date = (today_date1 - timedelta(days=1)).strftime("%Y%m%d")

    # Create an object of the class
    cmutils = Commonutils()

    # Input file path
    input_file_path = fr"{base_path}\{source_name}\airport.csv"  # CSV file path

    # Creating schema and reading input file into DataFrame
    input_schema = cmutils.generate_bronze_schema(schema_base_path, source_name)
    new_df = spark.read.csv(input_file_path, schema=input_schema, header=True)

    # Get the column name from the given table
    col_name = cmutils.get_column_name(schema_base_path, source_name)

    # Save and Read the files
    loading_path = fr"{loading_base_path}\{layer}\{source_name}\{today_date}"
    reading_path = fr"{loading_base_path}\{layer}\{source_name}\{yesterday_date}"
    loading_bad_data = r"C:\Users\kagar\pyspark_projects\AIRLINE_FINAL_PROJECT\output\bad_data1"
    loading_bad_data_path = fr"{loading_bad_data}\{layer}\{source_name}\{today_date}_bad_data"

    # Handling airline data: Apply null check and other quality checks
    if source_name == "airport":
        print(f"#--------------------{source_name}-------------------------#")

        # Applying null check and separating good and bad data
        good_data, bad_data = cmutils.apply_null_check(new_df, col_name)
        print(f"{source_name} good data count: ", good_data.count())
        print(f"{source_name} bad data count: ", bad_data.count())

        # Saving bad data
        bad_data.write.csv(loading_bad_data_path, mode="overwrite", header=True)

        # Processing good data
        if os.path.exists(reading_path):
            data = cmutils.scd_type_1(reading_path, source_name, good_data)
            print(f"Total {source_name} data (existing and today): {data.count()}")
            data.write.parquet(loading_path, mode="overwrite")
            cmutils.renaming_files(loading_base_path, layer, source_name, today_date)
            shutil.rmtree(reading_path)
        else:
            good_data.coalesce(1).write.parquet(loading_path, mode="overwrite")
            cmutils.renaming_files(loading_base_path, layer, source_name, today_date)
