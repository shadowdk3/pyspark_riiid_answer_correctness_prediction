import os
import sys

from dataclasses import dataclass

from src.exception import CustomException
from src.logger import logging

import src.utils

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import (
    LogisticRegression, 
    RandomForestClassifier, 
    GBTClassifier, 
    DecisionTreeClassifier
)

@dataclass
class ModelTrainerConfig:
    trained_model_file_path=os.path.join('artifacts', 'model')

class ModelTrainer:
    def __init__(self):
        self.model_trainer_config=ModelTrainerConfig()
        
    def initiate_model_trainer(self, train_data_transformed, test_data_transformed):
        try:
            classifiers = {
                "Logistic Regression": LogisticRegression(labelCol="answered_correctly", featuresCol="features"),
                # "Random Forest": RandomForestClassifier(labelCol="answered_correctly", featuresCol="features"),
                # "Gradient Boosting": GBTClassifier(labelCol="answered_correctly", featuresCol="features"),
                # "Decision Tree": DecisionTreeClassifier(labelCol="answered_correctly", featuresCol="features")
            }
            
            # Define the evaluator
            evaluator = BinaryClassificationEvaluator(labelCol="answered_correctly")

            # Dictionary to hold model performance
            model_metrics = {}

            best_model = None
            best_model_name = None
            best_accuracy = 0.0

            # Iterate over each classifier, train, evaluate and log the results
            for model_name, classifier in classifiers.items():
                logging.info(f"Training {model_name} Model:")
                
                # Fit the model on training data
                model = classifier.fit(train_data_transformed)
                
                # Make predictions on test data
                predictions = model.transform(test_data_transformed)
                
                # Evaluate the model
                accuracy = evaluator.evaluate(predictions)
                logging.info(f"{model_name} Accuracy: {accuracy}")
                
                # Save the performance metrics
                model_metrics[model_name] = accuracy
                
                # Keep track of the best model
                if accuracy > best_accuracy:
                    best_accuracy = accuracy
                    best_model_name = model_name
                    best_model = model

            # Log the best model's accuracy
            logging.info(f"Best Model: {best_model} with Accuracy: {best_accuracy}")

            src.utils.save_spark_object(
                file_path = self.model_trainer_config.trained_model_file_path, 
                obj=best_model)
            
            return (
                best_model_name,
                best_accuracy
            )
        
        except Exception as e:
            raise CustomException(e, sys)
