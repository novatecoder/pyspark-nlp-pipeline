# PySpark NLP Pipeline

PySpark 환경을 위한 Skew handling 및 NLP 전처리 파이프라인 라이브러리입니다.
`tomli` 의존성을 제거하고 가벼운 구조로 변경되었습니다.

## 📁 프로젝트 구조

```text
pyspark-nlp-pipeline/
├── src/
│   └── pyspark_nlp_pipeline/
│       ├── __init__.py
│       ├── cli.py      # Click 기반 커맨드라인 인터페이스
│       └── etl.py      # SkewedNLPPipeline 클래스 (Salting, Cleaning)
├── tests/
│   └── test_integration.py # 통합 테스트
├── pyproject.toml      # 프로젝트 설정 및 의존성
└── README.md
````

## 🚀 설치 방법 (Installation)

### 개발 모드로 설치 (권장)

코드를 수정하면 재설치 없이 바로 반영됩니다.

```bash
pip install -e .
```

### 개발 도구 포함 설치 (테스트 실행용)

```bash
pip install -e ".[dev]"
```

## 🧪 테스트 방법 (Testing)

`pytest`를 사용하여 통합 테스트를 실행합니다.

```bash
pytest -vv
```


## 💻 실행 방법 (Usage)

### 1\. CLI로 실행하기

터미널에서 명령어로 파이프라인을 실행할 수 있습니다.

```bash
# 기본 도움말 확인
pyspark-nlp-pipeline --help

# 기본 실행
pyspark-nlp-pipeline

# 옵션 지정 실행
pyspark-nlp-pipeline --input-path ./my_data --output-path ./result --salt-partitions 20
```

### 2\. Python 코드로 실행하기 (Library)

`config` 딕셔너리를 통해 설정을 주입할 수 있습니다.

```python
from pyspark_nlp_pipeline import SkewedNLPPipeline

# 설정 정의 (선택 사항)
config = {
    "app_name": "MyCustomApp",
    "salt_partitions": 50,
    "shuffle_partitions": 400
}

# 파이프라인 초기화
pipeline = SkewedNLPPipeline(config=config)

# 실행 로직
df = pipeline.read_data("./data/input.csv")
processed_df = pipeline.process_data(df)
pipeline.save_data(processed_df, "./data/output")
```

