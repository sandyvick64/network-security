from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.components.data_validation import DataValidation

from networksecurity.entity.config_entity import DataIngestionConfig, DataValidationConfig, TrainingPipelineConfig

from networksecurity.exceptions.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
import sys

if __name__ == "__main__":
    try:
        training_pipeline_config = TrainingPipelineConfig()
        data_ingestion_config = DataIngestionConfig(training_pipeline_config)
        data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
        logging.info("Data ingestion pipeline initiated")
        dataingestionartifact = data_ingestion.initiate_data_ingestion()
        logging.info("Data ingestion pipeline completed")
        logging.info(dataingestionartifact)

        # Data validation
        data_validation_config = DataValidationConfig(training_pipeline_config=training_pipeline_config)
        data_validation = DataValidation(data_ingestion_artifact=dataingestionartifact, data_validation_config=data_validation_config)
        logging.info("data validation initiated")
        datavalidationartifact = data_validation.initiate_data_validation()
        logging.info("data validation completed")
        logging.info(dataingestionartifact)
        
    except Exception as e:
        raise NetworkSecurityException(e, sys)      