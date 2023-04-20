#---------write_class.py
# this pyhon sheet contains the class Write with the following functions:
#---------------------------------------------------
#write_to_hadoop(self,table_sdf,table_name:str,partition_list:list,
#                          table_writing_mode:str)
#write_to_azure(self, table_sdf,storage:str,table_name:str,
#                          partition_list:list,
#                          table_writing_mode:str)
#write_table(self, sdf,table_name,
#                    partition_list, table_writing_mode,storage: str= None)
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



class Write():

    def __init__(self,
                sc,#: SparkSession, 
                db_part,
                env,
                platform,
                mount_point,
                storage_list,
                platform_list
                ):

        self.sc = sc
        self.db_part = db_part
        self.env = env
        self.platform = platform
        self.mount_point = mount_point
        self.storage_list=storage_list
        self.platform_list=platform_list
    
    @classmethod
    def from_config(cls, sc,config):
        db_part = config.get("db_part")
        env = config.get("env")
        platform = config.get("platform")
        mount_point = config.get("mount_point")
        storage_list = config.get("storage_list")
        platform_list = config.get("platform_list")
        
        return cls(sc,db_part,env,platform,
                   mount_point,storage_list,platform_list)

    
    
    def write_to_hadoop(self,table_sdf,table_name:str,partition_list:list,
                           table_writing_mode:str):
        """
        This function writes a table in hadoop and
        should only be used via the write_table-function!!

        """
        if partition_list == None:
            table_sdf.write\
                    .mode(table_writing_mode)\
                    .saveAsTable(self.db_part+ self.env+'.'+table_name)
        else:
            table_sdf.write.partitionBy(partition_list)\
                        .mode(table_writing_mode)\
                        .saveAsTable(self.db_part+ self.env+'.'+table_name)

        print("Table: "+self.db_part+ self.env+"."+table_name+" was successfully "+table_writing_mode+".")

        return table_sdf 

    def write_to_azure(self, table_sdf,storage:str,table_name:str,
                          partition_list:list,
                          table_writing_mode:str): #put dbutils in self? 
        #partition type needs to be a list !!
        """
        This function writes a table in azure and should only be used via the write_table-function!!

        """   
        assert storage in self.storage_list, f"The selected Storage_name {storage} is not a storage in this satalite {self.storage_list}."

        location_of_table= self.mount_point+"/"+storage+"/"+self.db_part+ self.env+"/"+table_name+"/"#+self.db_part+ self.env

        if partition_list == None:
            table_sdf.write\
                    .mode(table_writing_mode)\
                    .option("path", location_of_table)\
                    .saveAsTable(self.db_part+ self.env+'.'+table_name)
        
        else:     
            table_sdf.write\
                    .partitionBy(partition_list)\
                    .mode(table_writing_mode)\
                    .option("path", location_of_table)\
                    .saveAsTable(self.db_part+ self.env+'.'+table_name)

        print("Table: "+self.db_part+ self.env+'.'+table_name+" was successfully "+table_writing_mode+".")

        return table_sdf  

    def write_table(self, sdf,table_name,
                    partition_list, table_writing_mode,storage: str):
        """
        This function should be used when writing a table.
        This function was origenaly created to transform the writing of a table
        in a way that the codebase can be used for both the hadoop and the azure platform.
        The function write_to_azure and the function write_to_hadoop are used to
        create the tables on the respective system.

        """
        self.sc.sql(f"""CREATE DATABASE IF NOT EXISTS {self.db_part+ self.env}""")
 
        assert self.platform in self.platform_list, f"The selected Platform {self.platform} is not a used platform( {self.platform_list})."

        if self.platform == "azure":
            return self.write_to_azure(sdf,storage,
                                     table_name, partition_list,
                                     table_writing_mode)

        elif self.platform == "hadoop":
            return self.write_to_hadoop(sdf,table_name,
                                     partition_list,
                                     table_writing_mode)
        else:
            print(self.platform)
            raise("Wrong platform name !!")
            
