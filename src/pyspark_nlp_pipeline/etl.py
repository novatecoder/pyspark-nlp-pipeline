import tomli
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class SkewedNLPPipeline:
    # 기본 설정값 (Config 파일이 없을 경우 대비)
    DEFAULT_CONFIG = {
        "app_name": "DefaultSkewPipeline",
        "salt_partitions": 10,
        "shuffle_partitions": 200
    }

    def __init__(self, config_path: str = "pyproject.toml"):
        self.config = self._load_config(config_path)
        self.spark = SparkSession.builder \
            .appName(self.config.get("app_name", "SkewApp")) \
            .config("spark.sql.shuffle.partitions", self.config.get("shuffle_partitions", 200)) \
            .getOrCreate()
            
    def _load_config(self, path: str) -> dict:
        """설정 로드: 파일이 없거나 에러 발생 시 기본값 반환"""
        try:
            if os.path.exists(path):
                with open(path, "rb") as f:
                    toml_data = tomli.load(f)
                    return toml_data.get("tool", {}).get("pipeline_config", self.DEFAULT_CONFIG)
        except Exception as e:
            print(f"Warning: Failed to load config from {path}. Using defaults. Error: {e}")
        
        return self.DEFAULT_CONFIG

    def read_data(self, input_path: str) -> DataFrame:
        schema = StructType([
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("text", StringType(), True)
        ])
        return self.spark.read.csv(input_path, header=True, schema=schema)

    def process_data(self, df: DataFrame) -> DataFrame:
        """
        [Skew Handling & NLP Preprocessing]
        1. Salting -> 2. Repartition -> 3. Cache -> 4. Cleaning
        """
        salt_count = self.config.get("salt_partitions", 10)

        # 1. Salting
        df_salted = df.withColumn("salt_key", (F.rand() * salt_count).cast("int"))

        # 2. Repartitioning
        df_repartitioned = df_salted.repartition(salt_count, "salt_key")

        # 3. Caching
        df_repartitioned.cache()

        # 4. NLP Cleaning
        df_cleaned = df_repartitioned.withColumn(
            "cleaned_text",
            F.trim(F.regexp_replace(F.lower(F.col("text")), "[^a-z0-9\\s]", ""))
        )

        return df_cleaned.drop("salt_key")

    def save_data(self, df: DataFrame, output_path: str):
        df.write.mode("overwrite").parquet(output_path)