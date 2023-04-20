#-----------------------------------------------------------------------------------------------------------
#write_hadoop_parquet(self,parquet_sdf,parquet_name:str,partition_list:list,
#                           parquet_writing_mode:str, path:str)
#write_azure_parquet(self, parquet_sdf,storage:str,parquet_name:str,
#                          partition_list:list,
#                          parquet_writing_mode:str,
#                           path:str)
#write_parquet(self, parquet_sdf,parquet_name:str,
#                    partition_list:list, parquet_writing_mode:str, path:str, storage: str= None)
#---------------------------------------------------------------------------------------------------------------

class Writeparquet():

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


def write_hadoop_parquet(self,parquet_sdf,parquet_name:str,partition_list:list,
                           parquet_writing_mode:str, path:str):
        """
        This function writes a table in hadoop and
        should only be used via the write_table-function!!

        """
        location_of_parquet= path+"/"+parquet_name+"/"
        
        if partition_list == None:
            parquet_sdf.write\
                    .mode(parquet_writing_mode)\
                    .parquet(location_of_parquet)
        else:
            parquet_sdf.write.partitionBy(partition_list)\
                        .mode(parquet_writing_mode)\
                        .parquet(location_of_parquet)

        print("Parquet files are successfully "+parquet_writing_mode+". Location: "+location_of_parquet)

        return parquet_sdf 

    def write_azure_parquet(self, parquet_sdf,storage:str,parquet_name:str,
                          partition_list:list,
                          parquet_writing_mode:str,
                           path:str): #put dbutils in self? 
        #partition type needs to be a list !!
        """
        This function writes a table in azure and should only be used via the write_parquet-function!!

        """   
        assert storage in self.storage_list, f"The selected Storage_name {storage} is not a storage in this satalite {self.storage_list}."

        location_of_parquet= self.mount_point+"/"+storage+"/"+path+"/"+parquet_name+"/"#+self.db_part+ self.env

        if partition_list == None:
            parquet_sdf.write\
                    .mode(parquet_writing_mode)\
                    .parquet(location_of_parquet)
        
        else:     
            parquet_sdf.write.partitionBy(partition_list)\
                        .mode(parquet_writing_mode)\
                        .parquet(location_of_parquet)

        print("Parquet files are successfully "+parquet_writing_mode+". Location: "+location_of_parquet)

        return parquet_sdf  

    def write_parquet(self, parquet_sdf,parquet_name:str,
                    partition_list:list, parquet_writing_mode:str, path:str, storage: str):
        """
        This function should be used when writing a parquet file.
        This function was origenaly created to transform the writing of a parquet file
        in a way that the codebase can be used for both the hadoop and the azure platform.
        The function write_azure_parquet and the function write_hadoop_parquet are used to
        create the tables on the respective system.
        !!Please understand that the path that !!

        """
         
        assert self.platform in self.platform_list, f"The selected Platform {self.platform} is not a used platform( {self.platform_list})."

        if self.platform == "azure":
            return self.write_azure_parquet(parquet_sdf, storage, parquet_name, partition_list,
                          parquet_writing_mode, path)

        elif self.platform == "hadoop":
            return self.write_hadoop_parquet(parquet_sdf, parquet_name, partition_list,
                           parquet_writing_mode, path)
        else:
            print(self.platform)
            raise("Wrong platform name !!") 
            
