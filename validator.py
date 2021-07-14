from datetime import datetime
from typing import Optional

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatabaseStoreBackendDefaults
)


class DataQualityValidator(object):

    def __init__(
            self,
            asset_name: str = "asset_name",
            exp_name: Optional[str] = None,
            run_id: Optional[str] = None
    ):
        self._run_id = run_id or datetime.utcnow().strftime("%Y%m%dT%H%M%s")
        self._exp_name = exp_name or "RiskGE_" + self._run_id
        self.__expectation = None
        self._asset_name = asset_name
        self._exp_suite = None
        self._batch_request = None
        self.__validation_operators = None
        self.__datadocs_sites = None
        self.__ge_arguments = None

        self._context = self.__get_datacontext()

    @staticmethod
    def __get_credential(conn_id):
        return {
            "drivername": "postgres",
            "host": "192.168.99.100",
            "port": 5432,
            "username": "admin",
            "password": "admin",
            "database": "ge",
            "schema": "great_expectations"
        }

    def set_arguments(self, args):
        self.__ge_arguments = args

    def __setup_arguments(self):
        if self.__ge_arguments is None:
            arguments = {}
            self.__ge_arguments = arguments

    def set_datadocs_sites(self, sites):
        self.__datadocs_sites = sites

    def __get_datadocs_sites(self):
        if self.__datadocs_sites is None:
            self.__datadocs_sites = {
                "local_sites": {
                    "class_name": "SiteBuilder",
                    "stored_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": self.__ge_arguments.get('datadocs_base_dir') or '/tmp/ge'
                    },
                    "site_index_builder": {
                        "class_name": "DefaultSiteIndexBuilder"
                    }
                }
            }
        return self.__datadocs_sites

    def set_validation_operators(self, operators):
        self.__validation_operators = operators

    def __get_validation_operators(self):
        print('get validation operators')
        if self.__validation_operators is None:
            self.__validation_operators = {
                "action_list_operator": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [
                        {
                            "name": "store_validation_result",
                            "class_name": "StoreValidationResultAction"
                        },
                        {
                            "name": "store_evaluation_params",
                            "class_name": "StoreEvaluationParametersAction"
                        },
                        {
                            "name": "update_data_docs",
                            "class_name": "UpdateDataDocsAction"
                        }
                    ]
                }
            }
        return self.__validation_operators

    @property
    def __spark_datasource(self) -> dict:
        print('_ get spark datasource')
        config = {
            "name": "spark_df",
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SparkDFExecutionEngine",
                "module_name": "great_expectations.execution_engine"
            },
            "data_connectors": {
                "spark_df": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["run_id", "pipeline_stage"]
                }
            }
        }
        return config

    def _create_batch_request(self, df) -> RuntimeBatchRequest:
        print('create batch request')
        batch_request = RuntimeBatchRequest(
            datasource_name="spark_df",
            data_connector_name="spark-df",
            data_asset_name=self._asset_name,
            runtime_parameters={
                "batch_data": df
            },
            batch_identifiers={"pipeline_stage": "staging", "run_id": self._run_id},
        )
        return batch_request

    def __create_datacontext_config(self) -> DataContextConfig:
        print('# create data context configuration options')
        print('1/ setup database store backend defaults')
        if not self.__ge_arguments:
            self.__setup_arguments()
        credentials = self.__get_credential('greate_expectation_store')
        checkpoint_credentials = credentials.copy()
        checkpoint_credentials['table_name'] = 'checkpoint_store'
        checkpoint_credentials['key_columns'] = ['what', 'is', 'this']
        default_store = DatabaseStoreBackendDefaults(
            expectations_store_credentials=credentials,
            validations_store_credentials=credentials,
            checkpoint_store_credentials=checkpoint_credentials,
            expectations_store_name="expectations_store",
            validations_store_name="validations_store",
            evaluation_parameter_store_name="evaluations_store"
        )
        print('2/ start configure data context config')
        config = DataContextConfig(
            store_backend_defaults=default_store,
            data_docs_sites=self.__get_datadocs_sites(),
            validation_operators=self.__get_validation_operators(),
            anonymous_usage_statistics={"enabled": False}
        )
        print('3/ finish')
        return config

    def __get_datacontext(self) -> BaseDataContext:
        print('get data context')
        config = self.__create_datacontext_config()
        print('create data context with BaseDataContext and created config')
        context = BaseDataContext(config)
        print('data context is created')
        print('add data source')
        context.add_datasource(**self.__spark_datasource)
        return context

    def _get_validator(self, suite=None, batch_request=None):
        return self._context.get_validator(
            batch_request=batch_request or self._batch_request,
            expectation_suite=suite or self._exp_suite
        )

    def add_expectation(self, f):
        """register expectation to validator
        :param f: expectation (function)
        :return:
        """
        self.__expectation = f

    def validate(self, df):
        print('validating')
        self._exp_suite = self._context.create_expectation_suite(
            expectation_suite_name=self._exp_name, overwrite_existing=True
        )
        self._batch_request = self._create_batch_request(df)
        validator = self._get_validator()
        if self.__expectation is not None:
            self.__expectation(validator)
