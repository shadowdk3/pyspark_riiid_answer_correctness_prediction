import os
import sys
import io
from src.exception import CustomException
from src.logger import logging
from src.utils import PostgreSQLToSparkLoader
import src.utils

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, sum

from sklearn.model_selection import train_test_split

from src.components.data_transformation import DataTransformation
from src.components.model_trainer import ModelTrainer

import time

@dataclass
class SparkConfig:
    '''
    Limit Spark Memory Usage
    spark.memory.fraction: control how much memory is used for execution and storage
    spark.executor.memory: configure the memory for each executor
    spark.memory.storageFraction: determines the fraction of memory reserved for storing cached RDDs (or DataFrames)
    spark.default.parallelism: Too much parallelism can put pressure on memory
    spark.executor.cores: limiting the number of cores per executor
    spark.sql.shuffle.partitions: Reduce the number of partitions during shuffle-heavy operations like joins and aggregations
    spark.dynamicAllocation.enabled: enable dynamic allocation to automatically adjust the number of executors based on the workload
    '''
                        
    spark = SparkSession.builder \
        .appName('tutorial') \
        .config('spark.executor.memory', '4g') \
        .config('spark.driver.memory', '4g') \
        .config('spark.sql.shuffle.partitions', '50') \
        .config('spark.memory.fraction', '0.6') \
        .config('spark.sql.execution.arrow.pyspark.enabled', 'true') \
        .config("spark.jars", '/usr/local/lib/postgresql-42.2.24.jar') \
        .getOrCreate()

@dataclass
class DataIngestionConfig:
    train_data_path: str=os.path.join('artifacts', "train.csv")
    test_data_path: str=os.path.join('artifacts', "test.csv")
    data_path: str=os.path.join('artifacts', 'data.csv')
    raw_data_path: str=os.path.join('data', 'train.csv')
    questions_data_path: str=os.path.join('data', 'questions.csv')
    lectures_data_path: str=os.path.join('data', 'lectures.csv')
        
class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()
        self.loader = PostgreSQLToSparkLoader()
        
    def initiate_data_ingestion(self, spark):
        logging.info("Entered the data ingestion method or compont")
        try:
            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path), exist_ok=True)
            
            logging.info('Load dataset from postresql, read the dataset as dataframe')
            start_time = time.time()
            df = self.loader.load_data(
                spark=spark,
                table_name="raw_data",
                partition_column="row_id",  # Assume 'id' is a column used for partitioning
                lower_bound=1,
                upper_bound=1000000,
                num_partitions=10,
                fetch_size=5000
            )
            end_time = time.time()
            execution_time = end_time - start_time
            formatted_time = src.utils.format_duration(execution_time)
            logging.info(f'Execuate Time for load dataset form postresql: {formatted_time}')
            
            # logging.info('Load dataset from local, read the dataset as dataframe')
            # start_time = time.time()
            # df = spark.read.csv(self.ingestion_config.raw_data_path, header=True, inferSchema=True)
            # end_time = time.time()
            # execution_time = end_time - start_time
            # formatted_time = src.utils.format_duration(execution_time)
            # logging.info(f'Execuate Time for load dataset form local: {formatted_time}')
            
            sampled_df = df.sample(withReplacement=False, fraction=0.1)
            questions_df = spark.read.csv(self.ingestion_config.questions_data_path, header=True, inferSchema=True)
            lectures_df = spark.read.csv(self.ingestion_config.lectures_data_path, header=True, inferSchema=True)

            # 2. Join DataFrames
            logging.info('Join DataFrames')
            train_questions_df = sampled_df.join(questions_df, sampled_df.content_id == questions_df.question_id, how='left')
            full_df = train_questions_df.join(lectures_df, train_questions_df.content_id == lectures_df.lecture_id, how='left')

            # Select specific columns to avoid ambiguity
            full_df = full_df.select(
                "timestamp", "user_id", "content_id", "content_type_id", "task_container_id",
                "user_answer", "answered_correctly", "prior_question_elapsed_time", "prior_question_had_explanation",
                "question_id", "lecture_id",
                questions_df["part"].alias("question_part"),  # Rename question part
                lectures_df["part"].alias("lecture_part"),    # Rename lecture part
                "bundle_id", "correct_answer", "tags", "tag", "type_of"
            )

            # Capture the output
            old_stdout = sys.stdout  # Save the original stdout
            new_stdout = io.StringIO()  # Create a new StringIO object to capture the output
            sys.stdout = new_stdout  # Redirect stdout to the StringIO object

            full_df.printSchema()

            sys.stdout = old_stdout
            logging.info("%s", new_stdout.getvalue())
            
            logging.info("Handle nulls in numerical columns")
            logging.info("Fill nulls in 'prior_question_elapsed_time' with the mean value")
            mean_elapsed_time = full_df.select(mean(col('prior_question_elapsed_time'))).collect()[0][0]
            full_df = full_df.fillna({"prior_question_elapsed_time": mean_elapsed_time})

            logging.info("Fill nulls in 'prior_question_had_explanation' with 'False'")
            full_df = full_df.fillna({"prior_question_had_explanation": False})

            logging.info("Fill nulls in 'lecture_part' with -1 (indicating missing or lecture not applicable)")
            full_df = full_df.fillna({"lecture_part": -1})

            logging.info("Handle nulls in 'correct_answer' and 'question_part' (fill with -1 as placeholder for missing data)")
            full_df = full_df.fillna({"correct_answer": -1, "question_part": -1})
            
            full_df = full_df.filter(full_df.answered_correctly >= 0)
            
            full_df.select([col(c).isNull().alias(c) for c in full_df.columns]).show()

            logging.info("Dataframe after handle Null")

            new_stdout.truncate(0)
            sys.stdout = new_stdout  # Redirect stdout to the StringIO object
            full_df.printSchema()
            sys.stdout = old_stdout
            logging.info("%s", new_stdout.getvalue())
    
            train_data, test_data = full_df.randomSplit([0.8, 0.2], seed=42)
            
            train_df_pandas = train_data.toPandas()
            test_df_pandas  = test_data.toPandas()
            
            train_df_pandas.to_csv(self.ingestion_config.train_data_path, index=False, header=True)
            test_df_pandas.to_csv(self.ingestion_config.test_data_path, index=False, header=True)
            
            logging.info("data save.")

            return(
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path
            )
                    
        except Exception as e:
            raise CustomException(e, sys)
        
if __name__ == "__main__":
    spark = SparkConfig().spark
    
    obj = DataIngestion()
    train_data_path, test_data_path = obj.initiate_data_ingestion(spark)
    
    data_transformation = DataTransformation()
    train_data_transformed, test_data_transformed, _ = data_transformation.initiate_data_transformation(spark, train_data_path, test_data_path)

    modeltrainer = ModelTrainer()
    model_name, accuracy = modeltrainer.initiate_model_trainer(train_data_transformed, test_data_transformed)
    
    # Show model accuracy
    print(f"Model: {model_name} with Accuracy: {accuracy}")
            
    spark.stop()
    