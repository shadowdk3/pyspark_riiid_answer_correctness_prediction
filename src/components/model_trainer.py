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
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

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
                
                # Create a parameter grid for hyperparameter tuning
                param_grid = ParamGridBuilder() \
                    .addGrid(classifier.regParam, [0.01, 0.1, 1.0]) \
                    .addGrid(classifier.maxIter, [10, 50, 100]) \
                    .build()

                # Create a CrossValidator for hyperparameter tuning
                crossval = CrossValidator(estimator=classifier,
                                        estimatorParamMaps=param_grid,
                                        evaluator=evaluator,
                                        numFolds=5)  # 5-fold cross-validation

                # Fit the model using CrossValidator
                cv_model = crossval.fit(train_data_transformed)

                # Get the best model from cross-validation
                best_cv_model = cv_model.bestModel

                logging.info(f"Best Parameters for {model_name}:")
                logging.info(f" - Regularization Parameter (regParam): {best_cv_model.getRegParam()}")
                logging.info(f" - Max Iterations: {best_cv_model.getMaxIter()}")
                
                # Make predictions on test data
                predictions = best_cv_model.transform(test_data_transformed)
                
                # Evaluate the model
                accuracy = evaluator.evaluate(predictions)
                logging.info(f"{model_name} Accuracy: {accuracy}")
                
                # Save the performance metrics
                model_metrics[model_name] = accuracy
                
                # Keep track of the best model
                if accuracy > best_accuracy:
                    best_accuracy = accuracy
                    best_model_name = model_name
                    best_model = best_cv_model

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
