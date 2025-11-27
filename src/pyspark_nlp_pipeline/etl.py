import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class NLPPipeline:
    # 기본 설정값
    DEFAULT_CONFIG = {
        "app_name": "DefaultSkewPipeline",
        "salt_partitions": 10,
        "shuffle_partitions": 200
    }

    def __init__(self, config: dict = None):
        """
        초기화 메서드
        :param config: 파이프라인 설정 딕셔너리 (None일 경우 기본값 사용)
        """
        self.config = self.DEFAULT_CONFIG.copy()
        if config:
            self.config.update(config)

        self.spark = SparkSession.builder \
            .appName(self.config.get("app_name")) \
            .config("spark.sql.shuffle.partitions", self.config.get("shuffle_partitions")) \
            .getOrCreate()
            
    def read_data(self, input_path: str) -> DataFrame:
        schema = StructType([
            StructField("month", IntegerType(), True),
            StructField("day", IntegerType(), True),
            StructField("text", StringType(), True)
        ])
        # 파일이 없는 경우 등에 대한 처리는 Spark가 수행하지만,
        # 로컬 테스트 편의를 위해 경로 체크 로직을 추가할 수도 있음.
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