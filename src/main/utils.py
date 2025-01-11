from pyspark.sql.functions import *
from pyspark.sql.types import DateType, TimestampType
class Utils:
    def __init__(self, spark):
        self.spark = spark

    def makeUpper(self, str):
        return str.upper()

    def readExcel(self, loc, tabname):
        return self.spark.read.format("com.crealytics.spark.excel").option("dataAddress", f"'{tabname}'!A5").option("header", "true").option("inferSchema", "true").load(loc)

    def cleanDF(self, df):
        cleaned_df = df.toDF(*[cols.replace("\n", "").replace(" ", "_") for cols in df.columns])
        df_replaced = cleaned_df.select(*[when(col(c).isNull(), lit("")).otherwise(col(c)).alias(c) for c in cleaned_df.columns])
        df_replaced.show()
        return df_replaced

    def ifDateTimeStampThenConvert(self, df):
        date_format_str = "yyyy-MM-dd"
        timestamp_format_str = "yyyy-MM-dd HH:mm:ss"
        df_standardized = df.select(
            *[
                # Format DateType columns
                date_format(col(c), date_format_str).alias(c)
                if isinstance(df.schema[c].dataType, DateType)
                # Format TimestampType columns
                else date_format(col(c), timestamp_format_str).alias(c)
                if isinstance(df.schema[c].dataType, TimestampType)
                # Keep other columns unchanged
                else col(c)
                for c in df.columns
            ]
        )
        return df_standardized