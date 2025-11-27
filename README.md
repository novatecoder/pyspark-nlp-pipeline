# PySpark NLP Pipeline

PySpark 환경을 위한 확장 가능한 NLP 파이프라인 및 ETL 라이브러리입니다.

## 📁 프로젝트 구조

이 프로젝트는 `pyproject.toml`을 사용한 `src` 레이아웃을 따릅니다.

```

pyspark-nlp-pipeline/
├── src/
│   └── pyspark\_nlp\_pipeline/
│       ├── cli.py      \# 커맨드라인 인터페이스
│       ├── etl.py      \# ETL 데이터 처리 모듈
│       └── pipeline.py \# NLP 파이프라인 로직
├── pyproject.toml      \# 프로젝트 설정
└── README.md

````

## 🚀 설치 방법 (Installation)

### 개발 모드로 설치 (권장)
코드를 수정하면 재설치 없이 바로 반영됩니다.

```bash
pip install -e .
````

### 개발 도구 포함 설치 (테스트 등)

```bash
pip install -e ".[dev]"
```

## 💻 실행 방법 (Usage)

이 패키지는 **CLI(Command Line Interface)** 와 **라이브러리(Library)** 두 가지 방식으로 사용할 수 있습니다.

### 1\. CLI로 실행하기

설치가 완료되면 터미널에서 아래 명령어로 실행할 수 있습니다.

```bash
# 기본 도움말 확인
pyspark-nlp-pipeline --help

# 예시: ETL 작업 실행 (cli.py 구현에 따라 달라짐)
pyspark-nlp-pipeline run-etl --input-path ./data/input --output-path ./data/output
```

### 2\. Python 코드로 실행하기 (Library)

Jupyter Notebook이나 Python 스크립트에서 모듈을 직접 import하여 사용합니다.

#### ETL 모듈 사용 예시

```python
from pyspark.sql import SparkSession
from pyspark_nlp_pipeline.etl import ETLProcessor

spark = SparkSession.builder.appName("ETLApp").getOrCreate()

# ETL 클래스 사용
etl = ETLProcessor(spark)
df = etl.load_data("path/to/data")
processed_df = etl.transform(df)
processed_df.show()
```

#### Pipeline 모듈 사용 예시

```python
from pyspark_nlp_pipeline.pipeline import NLPPipeline

# 파이프라인 초기화
pipeline = NLPPipeline(input_col="text", output_col="features")

# 파이프라인 실행 로직...
```

## 🧪 테스트 방법 (Testing)

`pytest`를 사용하여 단위 테스트를 실행합니다.

```bash
# 전체 테스트 실행
pytest

# 상세 로그와 함께 실행
pytest -vv
```

## 🛠️ 요구 사항 (Requirements)

  - Python \>= 3.8
  - PySpark \>= 3.0.0
