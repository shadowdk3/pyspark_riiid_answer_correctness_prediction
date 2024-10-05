import sys
import pandas as pd

from src.exception import CustomException
import src.utils

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class PredicPipeline:
    def __init__(self):
        model_path = 'artifacts/model'
        preprocessor_path = 'artifacts/preprocessor'
        
        self.preprocessor  = src.utils.load_spark_pipeline(preprocessor_path)
        self.model = src.utils.load_logistic_regression_model(model_path)

    def predict(self, features):
        try:
            spark = SparkSession.builder \
                .appName('Pandas to Spark Conversion') \
                .getOrCreate()
    
            spark_df = spark.createDataFrame(features)
            
            # Cast columns to appropriate data types
            spark_df = spark_df.withColumn("timestamp", col("timestamp").cast("long")) \
                .withColumn("prior_question_elapsed_time", col("prior_question_elapsed_time").cast("double")) \
                .withColumn("user_id", col("user_id").cast("integer")) \
                .withColumn("content_id", col("content_id").cast("integer")) \
                .withColumn("task_container_id", col("task_container_id").cast("integer")) \
                .withColumn("user_answer", col("user_answer").cast("integer")) \
                .withColumn("question_part", col("question_part").cast("integer")) \
                .withColumn("lecture_part", col("lecture_part").cast("integer")) \
                .withColumn("bundle_id", col("bundle_id").cast("integer")) \
                .withColumn("correct_answer", col("correct_answer").cast("integer"))

            data_scaled = self.preprocessor.transform(spark_df)
            preds = self.model.transform(data_scaled)
            
            preds.select('features', 'prediction').show()
            prediction_values = preds.select('prediction').rdd.flatMap(lambda x: x).collect()
            
            return prediction_values
        
        except Exception as e:
            raise CustomException(e, sys)
class CustomData:
    def __init__(self,
        timestamp: int,
        user_id: int,
        content_id: int,
        task_container_id: int,
        user_answer: int,
        prior_question_elapsed_time: float,
        question_part: int,
        lecture_part: int,
        bundle_id: int,
        correct_answer: int,
        ):

        self.timestamp = timestamp
        self.user_id = user_id
        self.content_id = content_id
        self.task_container_id = task_container_id
        self.user_answer = user_answer
        self.prior_question_elapsed_time = prior_question_elapsed_time
        self.question_part = question_part
        self.lecture_part = lecture_part
        self.bundle_id = bundle_id
        self.correct_answer = correct_answer
        
    def get_data_as_data_frame(self):
        try:
            custom_data_input_dict = {
                "timestamp": [self.timestamp],
                "user_id": [self.user_id],
                "content_id": [self.content_id],
                "task_container_id": [self.task_container_id],
                "user_answer": [self.user_answer],
                "prior_question_elapsed_time": [self.prior_question_elapsed_time],
                "question_part": [self.question_part],
                "lecture_part": [self.lecture_part],
                "bundle_id": [self.bundle_id],
                "correct_answer": [self.correct_answer],
            }

            return pd.DataFrame(custom_data_input_dict)
        
        except Exception as e:
            raise CustomException(e, sys)