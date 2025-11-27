import pytest
import os
import shutil
from pyspark_nlp_pipeline import SkewedNLPPipeline

TEMP_INPUT_DIR = "./data/test_input"
TEMP_OUTPUT_DIR = "./data/test_output"

@pytest.fixture(scope="module")
def setup_teardown():
    os.makedirs(TEMP_INPUT_DIR, exist_ok=True)
    yield
    if os.path.exists(TEMP_INPUT_DIR):
        shutil.rmtree(TEMP_INPUT_DIR)
    if os.path.exists(TEMP_OUTPUT_DIR):
        shutil.rmtree(TEMP_OUTPUT_DIR)

def create_skewed_input_data(spark):
    data = []
    # Skewed Data (12/25)
    for i in range(1000):
        data.append((12, 25, f"Christmas Sale {i}!!!"))
    # Normal Data (1/1)
    for i in range(10):
        data.append((1, 1, f"New Year {i}"))

    columns = ["month", "day", "text"]
    df = spark.createDataFrame(data, columns)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(TEMP_INPUT_DIR)

def test_package_structure_and_pipeline(setup_teardown):
    """
    패키지 구조(src/pyspark_nlp_pipeline)가 정상 작동하는지 확인
    """
    # 1. Config 없이 초기화 (기본값 사용 테스트)
    # [수정] config_path 인자 제거, 필요시 config={} 전달
    pipeline = SkewedNLPPipeline()
    
    # 2. 데이터 생성
    create_skewed_input_data(pipeline.spark)
    
    # 3. 파이프라인 실행
    raw_df = pipeline.read_data(TEMP_INPUT_DIR)
    processed_df = pipeline.process_data(raw_df)
    pipeline.save_data(processed_df, TEMP_OUTPUT_DIR)
    
    # 4. 검증
    result_df = pipeline.spark.read.parquet(TEMP_OUTPUT_DIR)
    # 1000개 + 10개 = 1010개
    assert result_df.count() == 1010
    
    # NLP 로직 확인
    sample = result_df.filter("month=12").first()
    assert "christmas" in sample["cleaned_text"]
    assert "!!!" not in sample["cleaned_text"]
    
    print("\n✅ Library Structure & Pipeline Test Passed")