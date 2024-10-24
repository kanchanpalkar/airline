from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from utils_main import Commonutils
import argparse
import shutil
import os
#----------------------------------------------------------------------------------------

# Initialization Spark Session :-

if __name__ == '_main_':
    spark = SparkSession.builder.appName("AirLineMgmtSystem").master("local[*]").getOrCreate()

# -------------------------------------------------------------------------------------------------------------------------------------
    # Initialize parser using argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("-s", "--source_name",
                        default="airline",
                        help="please provide source name/dataset name")

    parser.add_argument("-sbp", "--schema_base_path",
                        default=r"E:\pyspark\project\airlinemgmtsystem\source_schema",
                        help="please provide Schema Base Path")
    parser.add_argument("-bs", "--base_path",
                        default=r"E:\pyspark\project\airlineproject_input",
                        help="please provide input Base Path")
    parser.add_argument("-l", "--layer",
                        default="Bronze",
                        help="please Provide layer name  Bronze/Silver/Gold name")
    parser.add_argument("-lp", "--loading_base_path",
                        default=r"E:\pyspark\project\airlinemgmtsystem",
                        help="please provide loading path ")
    parser.add_argument("-SCD_1", "--Do_u_want_to_apply_scd1",
                        default="True",
                        help="please provide input True or False")
# ----------------------------------------------------------------------------------------------------------------------
 #Create argument of parser
    args = parser.parse_args()

    # ----------------------------------------------------------------------------------------------------------------------
    # Initialization argument variables -:->

    source_name = args.source_name
    schema_base_path = args.schema_base_path
    base_path = args.base_path
    layer = args.layer
    loading_base_path = args.loading_base_path
    SCD_1 = args.Do_u_want_to_apply_scd1
    # ----------------------------------------------------------------------------------------------------------------------
    # Dates :- 1) Today_date   2) Yesterday_date

    # Get today file open
    today_date1 = (datetime.now() - timedelta(days=2))
    today_date = today_date1.strftime("%Y%m%d")

    # Get yesterday's date by subtracting 1 day from today's date and format it
    yesterday_date = (today_date1 - timedelta(days=1)).strftime("%Y%m%d")
    # ----------------------------------------------------------------------------------------------------------------------
    # Created object of the class
    cmutils = Commonutils()

    # Initialization path
    # E:\pyspark\project\airlineproject_input\<source_name>/yyyyMMdd/*csv
    # "E:\pyspark\project\airlineproject_input\airline\20231225"
    input_file_path = fr"{base_path}\{source_name}\{today_date}"

    # creating schema
    input_schema = cmutils.generate_bronze_schema(schema_base_path, source_name)
    # read the input file & stored in data frame
    new_df = spark.read.csv(input_file_path, input_schema)

    # Get the column name from given table
    col_name = cmutils.get_column_name(schema_base_path, source_name)
    # ----------------------------------------------------------------------------------------------------------------------
    # Save and Read the files :-
    # 1) Loading <Save the file on loading  location >
    # 2) Reading <Read the last day file path >
    # 3) load bad data

    # Saving to day file path
    loading_path = fr"{loading_base_path}\{layer}\{source_name}\{today_date}"
    print("Save the file :- ", loading_path)

    # Opening last day file  path
    reding_path = fr"{loading_base_path}\{layer}\{source_name}\{yesterday_date}"
    print("Read the file :- ", reding_path)

    loading_bad_data = r"E:\pyspark\project\airlineproject_output\bad_data"
    loading_bad_data_path = fr"{loading_bad_data}\{layer}\{source_name}\{today_date}_bad_data"
    # ----------------------------------------------------------------------------------------------------------------------

    if source_name == "airport":
        print(f"#--------------------{source_name}-------------------------#")

        # Check the special char and get good & bada from it
        good_data, bad_data = cmutils.special_char_check(new_df, col_name)
        print(f"{source_name} good data count :-", good_data.count())

        # bad_data Saving
        bad_data.write.csv(loading_bad_data_path, mode="ignore")

    elif source_name == "airline":
        print(f"#--------------------{source_name}-------------------------#")

        # apply_null_check and get good & bada from it
        good_data, bad_data = cmutils.apply_null_check(new_df, col_name)
        print(f"{source_name} good data count :-", good_data.count())

        # bad_data Saving
        bad_data.write.csv(loading_bad_data_path, mode="ignore")

    elif source_name == "route":
        print(f"#--------------------{source_name}-------------------------#")

        # "Check and replace with empty string" -->
        good_data = cmutils.replace_by_empty_string(new_df=new_df, col_name=col_name)
        print(f"{source_name} good data count :-", good_data.count())
        print(good_data.count())

    elif source_name == "plan":

        print(f"#--------------------{source_name}-------------------------#")
        # not apply any condition on plan
        good_data = new_df
        print(f"{source_name} good data count :-", good_data.count())
    # ----------------------------------------------------------------------------------------------------------------------

 # Save the Good Data  1) IF path exists the go (full_load,merge and append )
    #                     2) ELSE path does not exist then go to direct load

    # Checking the file is existing or not
    # if existing then go in side the function
    if os.path.exists(reding_path):
        # for airport, it will be merged & for airline it will Full_Load.
        if source_name == "airline" or "airport":
            data = cmutils.scd_type_1(reding_path, source_name, good_data)
            print(fr"total- {source_name} -data-existing-and-today ")
            print(data.count())
            data.show(1000)

            data.write.parquet(loading_path)
            cmutils.renaming_files(loading_base_path, layer, source_name, today_date)
            # "delete the previous  file or existing"
            shutil.rmtree(reding_path)

        # for route, it will be appended
        elif source_name == "route":
            data = cmutils.append_file(reding_path, good_data)
            print(fr"total- {source_name} -data-existing-and-today ")
            print(data.count())

            data.coalesce(1).write.parquet(loading_path)
            cmutils.renaming_files(loading_base_path, layer, source_name, today_date)
            # "delete the previous  file or existing"
            shutil.rmtree(reding_path)

    # Otherwise save the good data
    else:
        good_data.coalesce(1).write.parquet(loading_path, mode="overwrite")
        cmutils.renaming_files(loading_base_path, layer, source_name, today_date)






