import os
import sys

from dataclasses import dataclass

from src.exception import CustomException

from pyspark.ml import PipelineModel

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