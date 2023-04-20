#---------
# this pyhon sheet contains the class Read with the following functions:
#---------------------------------------------------
#read_parquet(self, path, parquet_name, storage: str)
#
#---------------------------------------------------


import pandas as pd
from datetime import datetime, timedelta
import decimal

from pyspark.sql import *
from pyspark import StorageLevel
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace
import pyspark.sql as sql
from pyspark.sql.types import LongType, IntegerType, BooleanType, DateType, FloatType, StringType, DecimalType, StructField, StructType, Row



class Read():

    def __init__(self,
                sc,#: SparkSession,   
                platform,
                mount_point,
                storage_list,
                platform_list
                ):

        self.sc = sc
        self.platform = platform
        self.mount_point = mount_point
        self.storage_list=storage_list
        self.platform_list=platform_list
    
    @classmethod
    def from_config(cls, sc,config):
        platform = config.get("platform")
        mount_point = config.get("mount_point")
        storage_list = config.get("storage_list")
        platform_list = config.get("platform_list")
        
        return cls(sc,platform,
                   mount_point,storage_list,platform_list)
        
        
    def read_parquet(self, path, parquet_name, storage: str):
        """
        This function should enable to read parquet files no matter which platform is used.
        """

        assert self.platform in self.platform_list, f"The selected Platform {self.platform} is not a used platform( {self.platform_list})."

        if self.platform == "azure":
            assert storage in self.storage_list, f"The selected Storage_name {storage} is not a storage in this satalite {self.storage_list}."

            location_of_parquet= self.mount_point+"/"+storage+"/"+path+"/"+parquet_name+"/"

            print("Parquet files are successfully read. Location: "+location_of_parquet)

            return self.sc.read.parquet(location_of_table)

        elif self.platform == "hadoop":
            location_of_table= path+"/"+parquet_name+"/"

            print("Parquet files are successfully read. Location: "+location_of_parquet)      

            return self.sc.read.parquet(location_of_table)

        else:
            print(self.platform)
            raise("Wrong platform name !!")
