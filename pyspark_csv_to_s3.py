from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import sys

def read_csv_and_write_to_s3(csv_input_path, s3_output_path, aws_access_key=None, aws_secret_key=None):
    """
    Read a CSV file, infer schema, and write to S3 location.
    
    Args:
        csv_input_path (str): Local file system path to the CSV file
        s3_output_path (str): S3 path to write the data (e.g., s3://bucket-name/path/)
        aws_access_key (str): AWS access key (optional if using IAM role)
        aws_secret_key (str): AWS secret key (optional if using IAM role)
    """
    
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("CSVtoS3") \
            .getOrCreate()
        
        # Configure AWS credentials if provided
        if aws_access_key and aws_secret_key:
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.access.key", aws_access_key
            )
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.secret.key", aws_secret_key
            )
        
        # Read CSV with schema inference
        print(f"Reading CSV file from: {csv_input_path}")
        df = spark.read \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .csv(csv_input_path)
        
        # Display inferred schema
        print("Inferred Schema:")
        df.printSchema()
        
        # Display sample data
        print(f"Total records: {df.count()}")
        print("\nSample data:")
        df.show(5, truncate=False)
        
        # Write to S3 in Parquet format (recommended for Spark)
        print(f"\nWriting data to S3: {s3_output_path}")
        df.write \
            .mode("overwrite") \
            .parquet(s3_output_path)
        
        print("Data successfully written to S3!")
        
        spark.stop()
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise


def read_csv_and_write_to_s3_csv_format(csv_input_path, s3_output_path, aws_access_key=None, aws_secret_key=None):
    """
    Alternative function to write output as CSV format instead of Parquet.
    
    Args:
        csv_input_path (str): Local file system path to the CSV file
        s3_output_path (str): S3 path to write the data
        aws_access_key (str): AWS access key (optional if using IAM role)
        aws_secret_key (str): AWS secret key (optional if using IAM role)
    """
    
    try:
        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("CSVtoS3CSV") \
            .getOrCreate()
        
        # Configure AWS credentials if provided
        if aws_access_key and aws_secret_key:
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.access.key", aws_access_key
            )
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.s3a.secret.key", aws_secret_key
            )
        
        # Read CSV with schema inference
        print(f"Reading CSV file from: {csv_input_path}")
        df = spark.read \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .csv(csv_input_path)
        
        # Display inferred schema
        print("Inferred Schema:")
        df.printSchema()
        
        # Write to S3 as CSV
        print(f"\nWriting data to S3: {s3_output_path}")
        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(s3_output_path)
        
        print("Data successfully written to S3 as CSV!")
        
        spark.stop()
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise


if __name__ == "__main__":
    # Example usage
    if len(sys.argv) > 2:
        input_csv = sys.argv[1]
        output_s3 = sys.argv[2]
        
        # Optional: AWS credentials
        aws_key = sys.argv[3] if len(sys.argv) > 3 else None
        aws_secret = sys.argv[4] if len(sys.argv) > 4 else None
        
        # Use Parquet format (recommended)
        read_csv_and_write_to_s3(input_csv, output_s3, aws_key, aws_secret)
        
        # Alternative: Use CSV format
        # read_csv_and_write_to_s3_csv_format(input_csv, output_s3, aws_key, aws_secret)
    else:
        print("Usage: python pyspark_csv_to_s3.py <input_csv_path> <s3_output_path> [aws_access_key] [aws_secret_key]")
        print("\nExample:")
        print("  python pyspark_csv_to_s3.py /path/to/file.csv s3://my-bucket/output/")
        print("  python pyspark_csv_to_s3.py /path/to/file.csv s3a://my-bucket/output/ AKIAIOSFODNN7EXAMPLE wJalrXUtnFEMI/K7MDENG+KEY")