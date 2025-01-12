from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from src.main.utils import Utils
from src.main.constants import *
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql.functions import current_timestamp
import os
import shutil
import logging
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO)



class Process:
    '''
    1. Read data
    2. Clean data
    3. Transform Data
    4. Save the result
    '''
    ...

def check_folder_contains_file(folder_path):
    logging.info("checking if the folder is empty..")
    for entry in os.listdir(folder_path):
        full_path = os.path.join(folder_path, entry)
        # Check if it's a file
        if os.path.isfile(full_path):
            return True
    return False

def list_files_in_directory(directory_path):
    # List all files in the directory
    return [file for file in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, file))]

def move(source, destination):
    filename = source.split("/")[-1]
    logging.info(f" File {filename} Moving from {source} to {destination}")
    shutil.copy2(source,destination)
    os.remove(source)


if __name__=="__main__":
    while True:
        time.sleep(5)
        logging.info("Scanning if the Landing zone got some new files..")
        if check_folder_contains_file(landing_zone):
            filename_to_process = list_files_in_directory(landing_zone)
            file_to_process_full_loc = landing_zone + "/" + filename_to_process[0]
            sparkCommonSession = SparkSession.builder.appName("Ptroines-compute").config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.7").getOrCreate()
            utilObject = Utils(sparkCommonSession)
            df_scanned = utilObject.readExcel(file_to_process_full_loc,excel_tab_name)
            df_scanned.show()
            df_replaced=utilObject.cleanDF(df_scanned)
            df_standardized = utilObject.ifDateTimeStampThenConvert(df_replaced)
            df_standardized = df_standardized.withColumn("Time of process", current_timestamp()).withColumn("Filename", lit(filename_to_process[0]))
            if df_scanned.count() == df_standardized.count():
                logging.info("Counts matching ..")
            df_standardized.show()
            '''result write to csv'''
            df_standardized.repartition(1).write.csv(output_result+"/"+filename_to_process[0].split(".")[0],mode="overwrite", header=True)
            '''backup'''
            backup_folder = Path(backup_location_of_all_processed_files)
            backup_folder.mkdir(parents=True, exist_ok=True)
            try:
                move(file_to_process_full_loc, backup_location_of_all_processed_files)
            except Exception as e:
                logging.info("exception found..",e)

