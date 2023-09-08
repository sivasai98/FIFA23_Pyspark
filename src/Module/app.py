import os
import sys

from pyspark.sql.functions import col, regexp_replace, expr, count
from src.constants.constants import HEADER, TRUE, DELIMITER, CSV_DELIMITER, INFERSCHEMA, CSV, empty_string, \
    REM_UN_CHAR1, REM_UN_CHAR2, REM_UN_CHAR3, REM_UN_CHAR4, case_st, NAN, INNER, CNT, FIFA23_OFFICIAL_DATA, \
    FIFA_CLUBS_DATA
from src.entity.fifa23_official_data import FOD, FC
from src.commons.spark_commons import get_spark_session


class App:
    def __init__(self):
        print("Init Method")

    def __enter__(self):
        """ creating Spark Session"""
        app_name = "RTB_C10"
        if 'spark' not in globals():
            print('No Spark Session exists in prior')
        self.spark = get_spark_session(app_name)
        print(app_name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ closing the spark context!"""
        print("spark session closed!!!")

    def read_csv(self, path):
        df = self.spark.read.format(CSV) \
            .option(HEADER, TRUE) \
            .option(DELIMITER, CSV_DELIMITER) \
            .option(INFERSCHEMA, TRUE) \
            .load(path)
        return df

    @staticmethod
    def data_cleaning(df1, fifa_clubs_df):
        try:
            drop_columns = [FOD.PHOTO, FOD.FLAG, FOD.CLUB_LOGO, FOD.BEST_OVER_ALL_RATING]
            df1 = df1.drop(*drop_columns) \
                .withColumn(FOD.VALUE, regexp_replace(FOD.VALUE, REM_UN_CHAR1, empty_string)) \
                .withColumn(FOD.WAGE, regexp_replace(FOD.WAGE, REM_UN_CHAR1, empty_string)) \
                .withColumn(FOD.POSITION, regexp_replace(FOD.POSITION, REM_UN_CHAR2, empty_string)) \
                .withColumn(FOD.LOANED_FROM, regexp_replace(FOD.LOANED_FROM, REM_UN_CHAR3, empty_string))
            fifa_clubs_df = fifa_clubs_df.withColumn(FC.CLUB, regexp_replace(FC.CLUB, REM_UN_CHAR4, empty_string))
            return df1, fifa_clubs_df
        except Exception as e:
            print(f"Error in data cleaning - {e}")
            raise

    def get_top_5_countries(self, df):
        try:
            print(f"In get_top_5_countries == {self.spark.sparkContext.appName}")
            return df.groupBy("Nationality") \
                .agg(count("Name").alias("count")) \
                .orderBy(col("count").desc()) \
                .limit(5)
        except Exception as e:
            print(f"Error in get top 5 countries - {e}")
            raise

    def get_role(self, df1):
        print(f"In get_role method ==={self.spark.sparkContext.appName}")
        try:
            return df1.withColumn(FOD.ROLE, expr(case_st)) \
                .select(FOD.NAME, FOD.CLUB, FOD.OVERALL, FOD.NATIONALITY, FOD.AGE, FOD.POSITION, FOD.ROLE)
        except Exception as e:
            print(f"Error in get role - {e}")
            raise

    def do(self):
        print(f"In Do method ==={self.spark.sparkContext.appName}")
        print("Reading Data !!!")
        fifa_official = self.read_csv(FIFA23_OFFICIAL_DATA)
        fifa_clubs = self.read_csv(FIFA_CLUBS_DATA)
        print("Data Cleaning!!!")
        df1, fifa_clubs_df = self.data_cleaning(fifa_official, fifa_clubs)
        print("Data Cleaning Done !!!")
        print("Processing Data !!!!")
        # get_top_5_countries
        print("Top 5 Countries")
        df2 = self.get_top_5_countries(df1)
        df2.show(truncate=False)

        # Added Role('ATTACKER', 'MIDFIELDER', 'DEFENDER', 'GOALKEEPER', 'SUBSTITUTE', 'RESERVE')
        print("get Role('ATTACKER', 'MIDFIELDER', 'DEFENDER', 'GOALKEEPER', 'SUBSTITUTE', 'RESERVE')")
        df3 = self.get_role(df1)
        df3.show(5, False)

        # identify players which are part of two clubs
        print("identify players which are part of two clubs")
        df4 = df1.where(col(FOD.LOANED_FROM) != NAN) \
            .select(FOD.NAME, FOD.LOANED_FROM, FOD.CLUB, FOD.AGE, FOD.NATIONALITY)
        df4.show(5, False)

        # find the name of the club which has maximum number of players on loan
        print("find the name of the club which has maximum number of players on loan")
        df5 = df4.groupBy(FOD.CLUB) \
            .agg(count(FOD.LOANED_FROM).alias(FOD.NUMBER_OF_PLAYERS_ON_LOAN)) \
            .orderBy(col(FOD.NUMBER_OF_PLAYERS_ON_LOAN).desc())
        df5.show(5, False)

        # get top 5 countries which are having the highest number of clubs
        print("get top 5 countries which are having the highest number of clubs")
        df6 = df1.join(fifa_clubs_df, [FOD.CLUB], INNER).select(FOD.CLUB, FC.COUNTRY) \
            .groupBy(FC.COUNTRY) \
            .agg(count(FOD.CLUB).alias(CNT)).orderBy(col(CNT).desc()).limit(5)
        df6.show(5, False)
