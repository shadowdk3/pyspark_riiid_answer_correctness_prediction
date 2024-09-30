import os
import sys
import io
from src.exception import CustomException
from src.logger import logging
import src.utils

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, sum

from sklearn.model_selection import train_test_split

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
        .getOrCreate()

@dataclass
class DataIngestionConfig:
    train_data_path: str=os.path.join('artifacts', "train.csv")
    test_data_path: str=os.path.join('artifacts', "test.csv")
    raw_data_path: str=os.path.join(src.utils.ProjectPathConfig().PROJECTPATH, 'data/train.csv')
    questions_data_path: str=os.path.join(src.utils.ProjectPathConfig().PROJECTPATH, 'data/questions.csv')
    lectures_data_path: str=os.path.join(src.utils.ProjectPathConfig().PROJECTPATH, 'data/lectures.csv')
        
class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()
        
    def initiate_data_ingestion(self, spark):
        logging.info("Entered the data ingestion method or compont")
        try:
            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path), exist_ok=True)
            logging.info('Read the dataset as dataframe')
                        
            df = spark.read.csv(self.ingestion_config.raw_data_path, header=True, inferSchema=True)
            df_sample = df.sample(withReplacement=False, fraction=0.1)
            questions_df = spark.read.csv(self.ingestion_config.questions_data_path, header=True, inferSchema=True)
            lectures_df = spark.read.csv(self.ingestion_config.lectures_data_path, header=True, inferSchema=True)
            
            # Combine DataFrames
            train_questions_df = df_sample.join(questions_df, df_sample.content_id == questions_df.question_id, how='left')
            full_df = train_questions_df.join(lectures_df, train_questions_df.content_id == lectures_df.lecture_id, how='left')

            logging.info("Combine questions.csv and lectures.csv with train.csv")
            
            # Capture the output
            old_stdout = sys.stdout  # Save the original stdout
            new_stdout = io.StringIO()  # Create a new StringIO object to capture the output
            sys.stdout = new_stdout  # Redirect stdout to the StringIO object

            full_df.printSchema()
            
            # Reset stdout to the original value
            sys.stdout = old_stdout

            # Get the output from StringIO
            logging.info("%s", new_stdout.getvalue())

            logging.info("Rename column name in 'part' in questions.csv and lectures.csv")
            logging.info("Remove column row_id")
            
            # Select specific columns to avoid ambiguity
            full_df = full_df.select(
                "row_id", "timestamp", "user_id", "content_id", "content_type_id", "task_container_id",
                "user_answer", "answered_correctly", "prior_question_elapsed_time", "prior_question_had_explanation",
                "question_id", "lecture_id",
                questions_df["part"].alias("question_part"),  # Rename question part
                lectures_df["part"].alias("lecture_part"),    # Rename lecture part
                "bundle_id", "correct_answer", "tags", "tag", "type_of"
            )

            logging.info("After select specific columns")
            
            new_stdout.truncate(0)
            sys.stdout = new_stdout  # Redirect stdout to the StringIO object
            full_df.printSchema()
            sys.stdout = old_stdout
            
            # Get the output from StringIO
            logging.info("%s", new_stdout.getvalue())

            logging.info("Filter out rows with -1 in the answered_correctly column")
            full_df = full_df.filter(full_df.answered_correctly >= 0)
            
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

            new_stdout.truncate(0)
            sys.stdout = new_stdout  # Redirect stdout to the StringIO object
            full_df.select([col(c).isNull().alias(c) for c in full_df.columns]).show()
            sys.stdout = old_stdout
            
            # Get the output from StringIO
            logging.info("%s", new_stdout.getvalue())
            
            logging.info("Train test split initiated")
            train_data, test_data = full_df.randomSplit([0.8, 0.2], seed=42)

            logging.info(f"Training Data Count: {train_data.count()}")
            logging.info(f"Testing Data Count: {test_data.count()}")
            
            # convert to pandas
            train_df_pandas = train_data.toPandas()
            test_df_pandas  = test_data.toPandas()
            
            train_df_pandas.to_csv(self.ingestion_config.train_data_path, index=False, header=True)
            test_df_pandas.to_csv(self.ingestion_config.test_data_path, index=False, header=True)
            
            logging.info("Ingestion of the data is completed")
        
            return(
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path,
            )
                    
        except Exception as e:
            raise CustomException(e, sys)
        
if __name__ == "__main__":
    spark = SparkConfig().spark
    
    obj = DataIngestion()
    train_data_path, test_data_path = obj.initiate_data_ingestion(spark)