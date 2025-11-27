# PySpark NLP Pipeline

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)](https://spark.apache.org/docs/latest/api/python/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green)](https://opensource.org/licenses/Apache-2.0)

**PySpark NLP Pipeline**은 대용량 텍스트 데이터를 처리할 때 발생하는 **데이터 쏠림(Data Skew)** 현상을 해결하고, 전처리 성능을 극대화하기 위해 설계된 데이터 엔지니어링 포트폴리오 프로젝트입니다.
표준 `pyproject.toml`을 통해 프로젝트 설정과 의존성을 통합 관리하며, Salting 기법과 Caching 전략을 통해 안정적인 파이프라인을 제공합니다.

## ✨ 주요 기능

이 시스템은 대규모 로그 및 텍스트 데이터 처리에 최적화된 3단계 프로세스를 수행합니다.

1.  **Skew Handling (Data Analyst Perspective)**
    * 특정 날짜나 키에 데이터가 몰리는 현상을 감지하고 해결
    * **Salting Strategy**: 임의의 난수(`salt_key`)를 생성하여 물리적 파티션을 강제로 균등 분배
    * `repartition()`을 통한 병렬 처리 효율 극대화

2.  **Performance Optimization (Engineer Perspective)**
    * **In-Memory Caching**: 반복적인 연산이 필요한 데이터프레임을 `cache()`하여 처리 속도 향상
    * **Columnar Storage**: 최종 산출물을 Parquet 포맷으로 저장하여 I/O 비용 및 용량 절감

3.  **NLP Preprocessing (Data Scientist Perspective)**
    * 정규표현식을 활용한 노이즈(특수문자, 공백) 제거
    * 대소문자 정규화 및 분석 가능한 형태의 텍스트 데이터 생성

## 🛠️ 요구 사항 (Requirements)

* Python >= 3.9
* Java 8+ (for Spark)
* 패키지 매니저: `pip` 또는 `uv` 등 (표준 `pyproject.toml` 지원 도구)

## 🚀 설치 및 설정 (Setup)

### 1. 의존성 설치

프로젝트 루트 폴더에서 다음 명령어를 실행하여 라이브러리를 설치합니다.

```bash
pip install .
# 또는 개발 의존성 포함 설치
pip install .[dev]

### 2. 프로젝트 설정 (`pyproject.toml`)

`config` 파일을 별도로 두지 않고, `pyproject.toml` 내의 `[tool.pipeline_config]` 섹션에서 파이프라인 설정을 통합 관리합니다.

**`pyproject.toml` 예시:**

```toml
[tool.pipeline_config]
app_name = "SkewOptimizationJob"
salt_partitions = 20    # Salting을 위한 파티션 계수
shuffle_partitions = 200
input_format = "csv"
output_format = "parquet"

## ✅ 테스트 실행 (Testing)

실제 데이터가 없어도 파이프라인을 검증할 수 있도록 **Mock Data**를 생성하여 E2E 테스트를 수행합니다.
테스트 코드는 임시 CSV 파일을 생성 -> 파이프라인 처리 -> Parquet 저장 -> 검증 과정을 자동으로 수행합니다.

```bash
pytest tests/test_integration.py -v

**테스트 시나리오:**
1.  `month=12, day=25`인 데이터를 1,000건 생성 (Skew 상황 연출)
2.  `month=1, day=1`인 데이터를 10건 생성
3.  파이프라인이 쏠림 현상 없이 데이터를 읽고, 정제하여 Parquet으로 저장하는지 검증
4.  데이터 손실 여부 및 텍스트 정제(소문자화 등) 정확도 확인

## 📚 파이프라인 구조 (Pipeline Structure)

| 단계 | 모듈 | 역할 및 최적화 기법 |
| :--- | :--- | :--- |
| **Configuration** | `pyproject.toml` | `tomli`를 사용하여 단일 파일에서 프로젝트 메타데이터와 파라미터 관리 |
| **Ingestion** | `read_data` | CSV/JSON 등 Raw Data 로드 (Schema Enforcement 적용) |
| **Optimization** | `process_data` | **Salting Key** 생성 → `repartition` (Shuffle) → `cache` (Memory) |
| **Transformation** | `process_data` | RegEx 기반 텍스트 클리닝 및 정규화 |
| **Load** | `save_data` | 압축률이 높은 **Parquet** 포맷으로 결과 저장 |

## 🖼️ 실행 흐름 (Workflow)

```mermaid
graph LR
    A[Raw CSV Data] --> B(Read & Schema Apply)
    B --> C{Add Salt Key}
    C --> D[Repartition via Salt]
    D --> E[Cache in Memory]
    E --> F[NLP Cleaning]
    F --> G[Drop Salt Key]
    G --> H[Write to Parquet]