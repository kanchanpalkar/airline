from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,StringType,StructField,BooleanType,DoubleType,DecimalType,StructType
from pyspark.sql.functions import lit,concat,regexp_extract,col,current_timestamp,when,row_number,length
from pyspark.sql.window import Window
import os
if __name__ == '_main_':

    spark = SparkSession.builder.master("local[*]").getOrCreate()

class Commonutils:
    def generate_bronze_schema( self ,base_path,source_name):
        """
        This method will generate pyspark schema for bronze layer Dataframe
        :param base_path : input base path name
        :param source_name: input dataset source name
        :return: schema
        """
        # initialize variables
        struct_field_list = []

        try:
            # Reading input schema files
            # "E:\pyspark\project\airlinemgmtsystem\source_schema\airline_schema.csv"
            file = open(fr"{base_path}\{source_name}_schema.csv", "r")

            # define all type in one dict
            type_of_data = {"integer": IntegerType(), "string": StringType(), "double": DoubleType(),"decimal": DecimalType(10,8), "boolean": BooleanType()}

            # broadcast_var = spark.sparkContext.broadcast(type_of_data)
            # # print(broadcast_var.value)
            # result = data_type.map(lambda x: broadcast_var.value[x])

            # Iterating over each schema element in input files
            for element in file.readlines():
                column_nm = element.split(",")[0]
                data_type = element.split(",")[1]

                # 1st methode to apply data type

                type_data = type_of_data.values(data_type.lower(),StringType())

                # 2nd methode to apply data types
                # if data_type.lower() == 'integer\n':
                #     type_data = IntegerType()
                # elif data_type.lower()  == "string":
                #     type_data = StringType()
                # elif data_type.lower()  == 'double':
                #     type_data = DoubleType()
                # elif data_type.lower()  == "boolean":
                #     type_data == BooleanType()
                # elif data_type.lower()  == "decimal":
                #     type_data = DecimalType()
                # else:
                #     type_data =StringType()

                # Generated Struct Filed values
                struct_field_list.append(StructField(column_nm, type_data))


            # Generated StructType scheama value
            input_schema = StructType(struct_field_list)
            file.close()
        except Exception as msg:
            return Exception(f"Error : While running generate_bronze_schema method {msg} ")

        return input_schema

    def get_column_name(self, file_path, source_name):
        """
         This methode gives you column name which is user defined
        :param file_path: This is schema path where we save column names ande data type
        :param source_name: input dataset source name
        :return:Columns names
        """
        try:
            # defind empty list to collect columns
            input_col_name = []

            # open Schema file
            file = open(f"{file_path}\{source_name}_schema.csv", "r")

            # comprehension
            # input_col_name = [line.split(',')[0] for line in file if "col" in line]

            for i in file:
                lst = i.split(',')
                if "col" in i:
                    input_col_name.append(lst[0])

            file.close()
        except Exception as obj:
            raise Exception(f"ERROR : File Was not found ,Failed with exception  {obj}")

        return input_col_name

    def special_char_check(self, df, col_name):
        """
         This is used to apply to check  special char in dataframe give bad ande good data
        :param df: This is data frame with values
        :param col_name: This is a column list which we can apply special char
        :return: good_data and bad_data
        """
        try:

            # pattern create
            special_char_cheak = r"([^A-Za-z|0-9|\|\s*])"
            # create new column
            result = df.withColumn("special_char", lit(""))

            for i in col_name:
                result = result.withColumn("special_char",
                                           concat("special_char", regexp_extract(col(f"{i}"), special_char_cheak, 1)))

            # another way to fetch the special char
            # df1 = df.withColumn("special_char", when(col("special_char") == "",
            # regexp_extract(f"{col_name}", special_char_cheak,1)).otherwise(col("special_char")))

            # To return the bad and good data to Bronze  files
            # Bad_data save in files
            bad_data = result.filter(col("special_char") != "")

            good_data = result.filter(col("special_char") == "")
            finall_good_data = good_data.drop("special_char")

            finall_good_data = finall_good_data.withColumn("time_stamp", lit(current_timestamp()))




        except Exception as obj:
            raise Exception(f"ERROR : While running special_char_check method.{obj} ")

        return finall_good_data, bad_data

    def scd_type_1(self, reding_path, source_name, new_df):

        """
        By using SCD1 to save the data in given files
        :param reding_path: To open las file for to day operation
        :param new_df: Good data of existing day
        :return: merge data of las day and today
        """
        try:

            ex_good_df = spark.read.parquet(reding_path)
            # ex_good_df.show()
            # new_df.show()

            data = new_df.union(ex_good_df)

            if source_name == "airport":
                windowspase = Window.partitionBy(col("airport_id")).orderBy(col("time_stamp").desc())
                data = data.withColumn("rk", row_number().over(windowspase))
                data1 = data.filter(col("rk") == 1)
                finall_data = data1.drop("rk")

                finall_data = finall_data.withColumn("airport_id", col("airport_id").cast("int")).sort(
                    col("airport_id").asc())

            elif source_name == "airline":
                windowspase = Window.partitionBy(col("Airline_ID")).orderBy(col("time_stamp").desc())
                data = data.withColumn("rk", row_number().over(windowspase))
                data1 = data.filter(col("rk") == 1)
                finall_data = data1.drop("rk")

                finall_data = finall_data.withColumn("Airline_ID", col("Airline_ID").cast("int")).sort(
                    col("Airline_ID").asc())


        except Exception as obj:
            raise print(obj)

        return finall_data

    def renaming_files(self, base_file_path, layer_name, source_name, date):
        """
        This method will rename the already saved .snappy.parquet file according to requirement
        :param base_file_path: This is the fixed path for parquet file
        :param layer_name: This is transformation layer name
        :param source_name: input dataset source_name
        :param date: Name of folder name
        :return:
        """

        file_path = fr"{base_file_path}\{layer_name}\{source_name}\{date}"
        current_filename = ''
        for filename in os.listdir(file_path):
            if ("snappy.parquet" in filename) and (".crc" not in filename):
                current_filename = filename
        new_filename = f"{source_name}_" + f"{date}" + ".parquet"
        current_filepath = os.path.join(file_path, current_filename)
        new_filepath = os.path.join(file_path, new_filename)
        try:
            os.rename(current_filepath, new_filepath)
            # print(f"File successfully renamed from {current_filename} to {new_filename}")
        except OSError as e:
            print(f"Error: While rename_files method Failed with {e}")

    def apply_null_check(self, new_df, column_list):
        """
        We have to check null in column_list and return good and bad data
        :param new_df:daily new file read
        :param column_list: column list for operations
        :return: good and bad data from  file
        """
        try:

            for i in column_list:
                data = new_df.withColumn(f'{i}',
                                         when(col(f'{i}').isNull(), '').when(col(f'{i}') == r'-1', '').otherwise(
                                             col(f'{i}')))

            data = data.withColumn("new_col", lit(""))
            for i in column_list:
                final_data = data.withColumn("new_col", when(col(f"{i}") == "", "True").otherwise(col("new_col")))

            final_data = final_data.filter(col("new_col") != "True").drop(col("new_col"))

            final_data1 = final_data.filter(length(col("IATA")) == 2).filter(length(col("ICAO")) == 3)

            finall_good_data = final_data1.withColumn("time_stamp", lit(current_timestamp()))

            # bad_data = new_df.join(final_data,"Airline_ID","leftanti")
            bad_data = new_df.exceptAll(final_data)

        except Exception as obj:
            raise Exception(f'Error while executing columns_to_check_special_char method as {obj}')
        return finall_good_data, bad_data

    def replace_by_empty_string(self, new_df, col_name):

        try:
            for i in col_name:
                data = new_df.withColumn(f"{i}",
                                         when(col(i) == r"\\N", "").when(col(i).isNull(), "").when(col(i) == "none",
                                                                                                   "").otherwise(
                                             col(i)))



        except Exception as obj:
            raise Exception(f'Error while executing replace_by_empty_string method as {obj}')

        return data

    def append_file(self, reding_path, new_df):
        """
        Use to append the to data yesterday and today
        :param reding_path: To open las file for to day operation
        :param new_df: Good data of existing day
        :return: merge data of las day and today
        """

        try:
            # last day file open in data frame
            ex_good_df = spark.read.parquet(reding_path)
            # apply union
            data = new_df.union(ex_good_df)


        except Exception as obj:
            raise print(f"Error while executing append_file method as {obj}")

        return data


