import os
import sys
import io
from dataclasses import dataclass

from src.exception import CustomException
from src.logger import logging
import src.utils

from pyspark.ml.feature import StandardScaler, VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql import functions as F

@dataclass
class DataTransformationConfig:
    preprocessor_obj_file_path=os.path.join('artifacts', "preprocessor")
    target='answered_correctly'
    
class DataTransformation:
    def __init__(self):
        self.data_transformation_config=DataTransformationConfig()

    def get_data_transformer_object(self):
        '''
        This function is responsible for data transformation in PySpark.
        It prepares the transformation pipeline.        
        '''
        try:
            # handleInvalid='keep': This setting ensures that any null values in the tags or type_of columns are assigned a new label, rather than throwing an error or skipping the row.
            tags_indexer = StringIndexer(inputCol="tags", outputCol="tags_indexed", handleInvalid='keep')
            type_of_indexer = StringIndexer(inputCol="type_of", outputCol="type_of_indexed", handleInvalid='keep')

            tags_encoder = OneHotEncoder(inputCol="tags_indexed", outputCol="tags_onehot")
            type_of_encoder  = OneHotEncoder(inputCol="type_of_indexed", outputCol="type_of_onehot")

            input_columns = [
                'timestamp', 
                'user_id', 
                'content_id', 
                'task_container_id', 
                'user_answer', 
                'prior_question_elapsed_time',
                'question_part', 
                'lecture_part', 
                'bundle_id', 
                'correct_answer', 
                # 'tags_onehot',
                # 'type_of_onehot'
            ]

            # Initialize the VectorAssembler with the updated column list
            assembler = VectorAssembler(
                inputCols=input_columns, 
                outputCol='features'
            )

            # Create a Pipeline with the stages
            preprocessor = Pipeline(stages=[
                # tags_indexer, 
                # type_of_indexer, 
                # tags_encoder, 
                # type_of_encoder, 
                assembler
            ])
    
            logging.info("Data transformation pipeline created successfully")

            return preprocessor
        
        except Exception as e:
            raise CustomException(e, sys)
        
    def initiate_data_transformation(self, spark, train_path, test_path):
        try:
            train_data = spark.read.csv(train_path, header=True, inferSchema=True)
            test_data = spark.read.csv(test_path, header=True, inferSchema=True)
            
            logging.info("Read train and test data completed")
            logging.info(f"Training Data Count: {train_data.count()}")
            logging.info(f"Testing Data Count: {test_data.count()}")
            
            logging.info("Obtaining preprocessing object")
            preprocessing_obj=self.get_data_transformer_object()

            pipeline_model = preprocessing_obj.fit(train_data)
            
            logging.info(
                f"Applying preprocessing object on training dataframe and testing dataframe."
            )

            train_data_transformed  = pipeline_model.transform(train_data)
            test_data_transformed  = pipeline_model.transform(test_data)

            logging.info("Data transformation completed on both training and testing data")

            logging.info(f"Saved preprocessing object.")
                        
            src.utils.save_spark_object(
                file_path = self.data_transformation_config.preprocessor_obj_file_path, 
                obj=pipeline_model)
            
            return(
                train_data_transformed,
                test_data_transformed,
                self.data_transformation_config.preprocessor_obj_file_path
            )
            
        except Exception as e:
            raise CustomException(e, sys)