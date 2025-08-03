from networksecurity.entity.artifact_entity import DataIngestionArtifact
from networksecurity.entity.artifact_entity import DataValidationArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exceptions.exception import NetworkSecurityException
from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.logging.logger import logging
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file
from scipy.stats import ks_2samp
import pandas as pd
import sys, os


class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact,
                 data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.dat_validation_config = data_validation_config
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    @staticmethod
    def read_file(file_path):
        try:
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    def validate_column(self, data_frame: pd.DataFrame)-> bool:
        try:
            no_column = len(self.schema_config)
            logging.info(f"Required no of columns: {no_column}")
            logging.info(f"No of Data Frame columns are:: {len(data_frame.columns)}")

            if len(data_frame.columns) == no_column:
                return True
            else:
                return False
        except Exception as e:
            raise NetworkSecurityException(e,sys)

    def detect_dataset_drift(self, base_df, current_df, threshold=0.05)-> bool:
        try:
            status=True
            report={}
            for column in base_df.columns:
                d1 = base_df[column]
                d2 = current_df[column]

                is_sample_dist = ks_2samp(d1,d2)

                if threshold <= is_sample_dist.pvalue:
                    is_found=False
                else:
                    is_found=True
                    status=True
                report.update(
                    {
                        column:{
                            "p_value": float(is_sample_dist.pvalue),
                            "drift_status": is_found
                        }
                    }
                )
                drift_report_file_path = self.dat_validation_config.drift_report_file_path

                # create directory
                dir_path = os.path.dirname(drift_report_file_path)
                os.makedirs(dir_path, exist_ok=True)

                write_yaml_file(file_path=drift_report_file_path, content=report, replace=True)
                return status

        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    def initiate_data_validation(self)-> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path
            
            #Read train and test
            train_df = DataValidation.read_file(train_file_path)
            test_df = DataValidation.read_file(test_file_path)

            #Validate columns
            train_status = self.validate_column(train_df)
            if not train_status:
                error_message = f"Train dataframe does noclst contain all columns.\n"
            
            test_status = self.validate_column(test_df)
            if not test_status:
                error_message = f"Test dataframe does not contain all columns.\n."
            
            # check data drift
            status = self.detect_dataset_drift(base_df=train_df, current_df=test_df)
            dir_path = os.path.dirname(self.dat_validation_config.valid_train_file_path)
            os.makedirs(dir_path, exist_ok=True)

            train_df.to_csv(
                self.dat_validation_config.valid_train_file_path, index=False, header=True
            )
            test_df.to_csv(
                self.dat_validation_config.valid_test_file_path, index=False, header=True
            )

            data_validation_artifect = DataValidationArtifact(
                validation_status = status,
                valid_train_file_path = self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path = self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path = None,
                drift_report_file_path = self.dat_validation_config.drift_report_file_path
            )
            return data_validation_artifect
        except Exception as e:
            raise NetworkSecurityException(e,sys)