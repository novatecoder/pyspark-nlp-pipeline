import sys
import os
from .etl import SkewedNLPPipeline

def main():
    """
    CLI 실행 함수
    명령어: run-pipeline
    """
    # 라이브러리로 설치되었을 때 현재 위치의 pyproject.toml을 찾기 위한 로직
    config_path = "pyproject.toml" if os.path.exists("pyproject.toml") else None
    
    pipeline = SkewedNLPPipeline(config_path)
    
    input_path = "./data/input"
    output_path = "./data/output"

    print(f"[{pipeline.config.get('app_name')}] Starting Pipeline...")

    try:
        if not os.path.exists(input_path):
            print(f"Error: Input path '{input_path}' not found.")
            return

        df = pipeline.read_data(input_path)
        processed_df = pipeline.process_data(df)
        pipeline.save_data(processed_df, output_path)
        
        print(f"Success! Data saved to: {output_path}")

    except Exception as e:
        print(f"Pipeline Failed: {e}")
        pipeline.spark.stop()
        raise

if __name__ == "__main__":
    main()