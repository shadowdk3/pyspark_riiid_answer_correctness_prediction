import os
import sys

from dotenv import load_dotenv
import pandas as pd

import psycopg2
import tqdm

from dataclasses import dataclass

from src.exception import CustomException

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
import psycopg2.extras

@dataclass
class ProjectPathConfig:
    FILEPATH = os.path.dirname(os.path.abspath(__file__))
    PROJECTPATH = os.path.dirname(FILEPATH)
    
def save_spark_object(file_path, obj):
    try:
        obj.write().overwrite().save(file_path)

    except Exception as e:
        raise CustomException(e, sys)
    
def load_spark_object(file_path):
    try:
        return PipelineModel.load(file_path)

    except Exception as e:
        raise CustomException(e, sys)

def format_duration(seconds):
    """Convert seconds to HH:MM:SS format."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"

@dataclass
class PostgreSQLConfig:
    load_dotenv()
    DRIVER = "org.postgresql.Driver"

class PostgreSQLToSparkLoader:
    def __init__(self):
        self.postgresqlconfig = PostgreSQLConfig()
        
        self.db_config = {
            'url': f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        
    def load_data(self, spark, table_name, partition_column=None, lower_bound=None, upper_bound=None, num_partitions=10, fetch_size=10000):
        """
        Load large data from PostgreSQL into Spark with partitioning.
        
        Parameters:
        table_name (str): The table name to load.
        partition_column (str): The column to partition the reads on (typically an indexed column).
        lower_bound (int): Lower bound for partition column (used for partitioning).
        upper_bound (int): Upper bound for partition column (used for partitioning).
        num_partitions (int): Number of partitions for parallel reading.
        fetch_size (int): Batch size for reading rows from PostgreSQL.

        Returns:
        DataFrame: A Spark DataFrame containing the data from PostgreSQL.
        """
        # JDBC connection properties
        jdbc_options = {
            "url": self.db_config['url'],
            "dbtable": table_name,
            "user": self.db_config['user'],
            "password": self.db_config['password'],
            "fetchsize": str(fetch_size),
            "driver": self.postgresqlconfig.DRIVER
        }
        
        # Add partitioning options if partition column is provided
        if partition_column:
            jdbc_options.update({
                "partitionColumn": partition_column,
                "lowerBound": str(lower_bound),
                "upperBound": str(upper_bound),
                "numPartitions": str(num_partitions)
            })
        
        # Load data into Spark DataFrame
        df = spark.read \
            .format("jdbc") \
            .options(**jdbc_options) \
            .load()
        
        print(f"Successfully loaded table {table_name} into Spark DataFrame.")
        return df