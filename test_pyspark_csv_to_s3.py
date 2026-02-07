import pytest
from pyspark.sql import SparkSession
import pandas as pd

@pytest.fixture(scope='module')
def spark():
    spark = SparkSession.builder.appName('test_pyspark_csv_to_s3').getOrCreate()
    yield spark
    spark.stop()

# Test cases to verify CSV reading functionalities

def test_csv_reading(spark):
    df = spark.read.csv('test_data/sample.csv', header=True, inferSchema=True)
    assert df.count() > 0, 'CSV file is empty'
    assert set(df.columns) == {'column1', 'column2', 'column3'}, 'Columns do not match'

# Test case to verify schema inference

def test_schema_inference(spark):
    df = spark.read.csv('test_data/sample.csv', header=True, inferSchema=True)
    assert df.schema['column1'].dataType.typeName() == 'string', 'Data type of column1 is not string'
    assert df.schema['column2'].dataType.typeName() == 'integer', 'Data type of column2 is not integer'

# Test case to verify Parquet writing functionalities

def test_parquet_write(spark):
    df = spark.read.csv('test_data/sample.csv', header=True, inferSchema=True)
    df.write.parquet('output_data/sample.parquet')
    parquet_df = spark.read.parquet('output_data/sample.parquet')
    assert parquet_df.count() == df.count(), 'The counts do not match'

# Test case to check CSV format

def test_csv_format():
    df = pd.read_csv('test_data/sample.csv')
    assert df.columns.tolist() == ['column1', 'column2', 'column3'], 'CSV format is incorrect'

# Test cases for error handling

def test_csv_read_invalid_path(spark):
    with pytest.raises(Exception):
        spark.read.csv('invalid_path/sample.csv')

# Test cases for data type handling

def test_data_type_handling(spark):
    df = spark.read.csv('test_data/sample.csv', header=True, inferSchema=True)
    assert isinstance(df.select('column2').head(1)[0]['column2'], int), 'column2 is not of type int'

# Integration test case

def test_integration(spark):
    df = spark.read.csv('test_data/sample.csv', header=True, inferSchema=True)
    df.write.parquet('output_data/integration_test.parquet')
    result_df = spark.read.parquet('output_data/integration_test.parquet')
    assert result_df.count() == df.count(), 'Integration test count does not match'
