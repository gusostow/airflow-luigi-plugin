from unittest import mock

import boto3
from moto import mock_s3

import airflow
from airflow.operators.luigi_plugin import LuigiOperator


@mock_s3
@mock.patch.object(airflow.hooks.S3_hook.S3Hook, "get_conn", new=lambda self: boto3.client("s3"))
def test_no_upstream(tmp_path):
    conn = boto3.client("s3")
    bucket_name = "mock_bucket"
    conn.create_bucket(Bucket=bucket_name)

    output_body = "test output"

    def transformer(upstream_input_paths=None, output_path=None, **context):
        with open(output_path, "w") as f:
            f.write(output_body)

    output_file_name = "mock_file"
    base_s3_key = "interim"
    t = LuigiOperator(
        task_id="no_upstream",
        transform_callable=transformer,
        bucket_name=bucket_name,
        output_file_name=output_file_name,
        base_s3_key=base_s3_key
    )

    t.execute(ti=mock.MagicMock())
    output_body_actual = conn.get_object(Bucket=bucket_name, Key=f"{base_s3_key}/{output_file_name}")["Body"].read().decode()

    assert output_body_actual == output_body
