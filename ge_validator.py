"""
The expectation and validation stores are configured with PostgresDB, there are 2 expectation suites
are generated based on the demo taxi data provided in GE tutorials.
Spark are used to load data of the sample data of 201902 and validate with suite created from the sample
of 201901.
"""
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext

import pandas as pd
from pprint import pprint
from datetime import datetime
from typing import Optional

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatabaseStoreBackendDefaults
)

# SPARK CONFIGURATION
def init_spark(application_name: str):
    if not application_name:
        raise Exception('Application name is undefined')
    SPARK_MASTER_HOST = 'localhost'
    SPARK_DRIVER_HOST = 'localhost'
    SPARK_PORT = 7077
    SPARK_UI_PORT = 8800
    conf = SparkConf()
    conf.setAll([
        ('spark.master', f'spark://{SPARK_MASTER_HOST}:{SPARK_PORT}'),
        ('spark.app.name', application_name),
        ('spark.driver.host', SPARK_DRIVER_HOST),
        ('spark.ui.port', SPARK_UI_PORT),
        ('spark.executor.memory', '2048m'),
        ('spark.driver.memory', '2g'),
        ('spark.driver.maxResultSize', '1g'),
        ('spark.executor.cores', 2),
        ('spark.cores.max', 8),
        ('spark.driver.extraClassPath', '/opt/spark/thirdparty_jars/*'),
        ('spark.executor.extraClassPath', '/opt/spark/thirdparty_jars/*')
    ])
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
    return spark, sqlContext
 
spark, sql_context = init_spark('ge_test')
df = spark.read.csv("/home/quanns/data/yellow_tripdata_sample_2019-02.csv", header=True)

# GREAT EXPECTATIONS CONFIGURATION
credentials = {
    "drivername": "postgres",
    "host": "localhost",
    "port": 5432,
    "username": "admin",
    "password": "admin",
    "database": "ge",
    "schema": "great_expectations"
}

datasource_config = {
    "spark_datasource": {
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine",
            "spark_config": {
                'spark.master': f'spark://localhost:7077',
                'spark.app.name': "great_expectations",
                'spark.driver.host': "localhost",
                'spark.ui.port': "8800",
                'spark.executor.memory': '2048m',
                'spark.driver.memory': '2g',
                'spark.driver.maxResultSize': '1g',
                'spark.executor.cores': 2,
                'spark.cores.max': 8,
                'spark.driver.extraClassPath': '/opt/spark/thirdparty_jars/*',
                'spark.executor.extraClassPath': '/opt/spark/thirdparty_jars/*',
            }
        },
        "data_connectors": {
            "runtime_data_connector": {
                "module_name": "great_expectations.datasource.data_connector",
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["dag_id", "execution_time"]
            }
        }
    }
}

stores_config = {
    "expectation_store": {
        "class_name": "ExpectationsStore",
        "store_backend": {
            "class_name": "DatabaseStoreBackend",
            "credentials": credentials
        }
    },
    "validation_store": {
        "class_name": "ValidationsStore",
        "store_backend": {
            "class_name": "DatabaseStoreBackend",
            "credentials": credentials
        }
    },
    "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
    "checkpoint_store": {
        "class_name": "CheckpointStore",
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "suppress_store_backend_id": True,
            "base_directory": "/tmp/ge/checkpoints/"
        }
    }
}

datadocs_config = {
    "local_site": {
        "class_name": "SiteBuilder",
        "show_how_to_button": True,
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory":  "/tmp/ge/datadocs/",
            "prefix":  "RISK_",
        },
        "site_index_builder": {
            "class_name": "DefaultSiteIndexBuilder",
            "show_cta_footer": True,
        },
    }
}


validation_operators = {
    "action_list_operator": {
        "class_name": "ActionListValidationOperator",
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
        ],
    }
}


project_config = DataContextConfig(
    config_version=3,
    plugins_directory=None,
    config_variables_file_path=None,
    datasources=datasource_config,
    stores=stores_config,
    expectations_store_name="expectation_store",
    validations_store_name="validation_store",
    checkpoint_store_name="checkpoint_store",
    evaluation_parameter_store_name="evaluation_parameter_store",
    data_docs_sites=datadocs_config,
    validation_operators=validation_operators,
    anonymous_usage_statistics={
      "enabled": False
    }
)

ge_context = BaseDataContext(project_config)
ge_context.list_expectation_suite_names()
# output
# ['suite_taxi_201902', 'suite_taxi_201901']

suite = ge_context.get_expectation_suite('suite_taxi_201901')
ge_context.list_datasources()
# output
"""
[{'data_connectors': {'runtime_data_connector': {'module_name': 'great_expectations.datasource.data_connector',
    'batch_identifiers': ['dag_id', 'execution_time'],
    'class_name': 'RuntimeDataConnector'}},
  'execution_engine': {'spark_config': {'spark.master': 'spark://10.109.2.4:7077',
    'spark.app.name': 'great_expectations',
    'spark.driver.host': '10.109.2.4',
    'spark.ui.port': '8800',
    'spark.executor.memory': '2048m',
    'spark.driver.memory': '2g',
    'spark.driver.maxResultSize': '1g',
    'spark.executor.cores': 2,
    'spark.cores.max': 8,
    'spark.driver.extraClassPath': '/opt/spark/thirdparty_jars/*',
    'spark.executor.extraClassPath': '/opt/spark/thirdparty_jars/*'},
   'class_name': 'SparkDFExecutionEngine'},
  'class_name': 'Datasource',
  'name': 'spark_datasource'}]
"""


batch_request = RuntimeBatchRequest(
    datasource_name="spark_datasource",
    data_connector_name="runtime_data_connector",
    data_asset_name="insert_your_data_asset_name_here",
    runtime_parameters={
      "batch_data": df_csv
    },
    batch_identifiers={
        "dag_id": "post_mortem_identify_fraud_trans",
        "execution_time": "20102724010100"
    }
)


validator = ge_context.get_validator(
    batch_request=batch_request, expectation_suite=suite
)

validator.expect_column_to_exist("hello")
# output
"""
{
  "result": {},
  "success": false,
  "exception_info": {
    "raised_exception": false,
    "exception_traceback": null,
    "exception_message": null
  },
  "meta": {}
}
"""

ge_context.run_validation_operator(
    validation_operator_name="action_list_operator",
    assets_to_validate=[validator],
    run_id="run_id"
)
