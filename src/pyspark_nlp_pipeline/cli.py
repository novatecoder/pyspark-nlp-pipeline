import sys
import os
import click
from .etl import SkewedNLPPipeline

@click.command()
@click.option('--input-path', default='./data/input', help='Path to input data CSV')
@click.option('--output-path', default='./data/output', help='Path to save processed Parquet data')
@click.option('--salt-partitions', default=10, help='Number of partitions for salting (skew handling)')
@click.option('--shuffle-partitions', default=200, help='Spark shuffle partitions')
def main(input_path, output_path, salt_partitions, shuffle_partitions):
    """
    PySpark NLP Pipeline CLI 실행
    """
    # 설정 딕셔너리 구성
    config = {
        "app_name": "SkewedNLPPipelineCLI",
        "salt_partitions": salt_partitions,
        "shuffle_partitions": shuffle_partitions
    }
    
    pipeline = SkewedNLPPipeline(config=config)
    
    print(f"[{pipeline.config.get('app_name')}] Starting Pipeline...")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")

    try:
        if not os.path.exists(input_path) and not input_path.startswith("file://") and not input_path.startswith("hdfs://") and not input_path.startswith("s3://"):
             print(f"Warning: Local input path '{input_path}' not found. Spark might fail if scheme is not provided.")

        df = pipeline.read_data(input_path)
        processed_df = pipeline.process_data(df)
        pipeline.save_data(processed_df, output_path)
        
        print(f"Success! Data saved to: {output_path}")

    except Exception as e:
        print(f"Pipeline Failed: {e}")
        pipeline.spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()