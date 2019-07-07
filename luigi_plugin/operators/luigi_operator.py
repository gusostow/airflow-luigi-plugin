from tempfile import NamedTemporaryFile

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


@apply_defaults
class LuigiOperator(BaseOperator):
    template_fields = ("output_file_name", "base_s3_key")

    def __init__(self,
                 transform_callable,
                 bucket_name,
                 output_file_name=None,
                 base_s3_key="interim",
                 aws_conn_id="aws_default",
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.transform_callable = transform_callable
        self.bucket_name = bucket_name
        self.output_file_name = output_file_name
        self.base_s3_key = base_s3_key
        self.aws_conn_id = aws_conn_id

    def execute(self, **context):
        s3_conn = S3Hook(aws_conn_id=self.aws_conn_id)

        upstream_input_paths = {}
        for upstream_task in self.upstream_list:
            # Get input S3 keys from upstream luigi operator tasks
            if upstream_task.task_type == "LuigiOperator":
                input_s3_key = context["ti"].xcom_pull(
                    key="output_s3_key", task_ids=upstream_task.task_id)
                with NamedTemporaryFile("w", delete=False) as temp_input_file:
                    key = s3_conn.get_key(input_s3_key,
                                          bucket_name=self.bucket_name)
                    key.download_fileobj(Fileobj=temp_input_file)
                    temp_input_file.flush()
                upstream_input_paths[upstream_task.task_id] = temp_input_file.name

        if self.output_file_name is not None:
            temp_output_path = NamedTemporaryFile("w", delete=False).name
            self.transform_callable(upstream_input_paths=upstream_input_paths,
                                    output_path=temp_output_path,
                                    **context)
            output_s3_key = f"{self.base_s3_key}/{self.output_file_name}"
            s3_conn.load_file(filename=temp_output_path,
                              key=output_s3_key,
                              bucket_name=self.bucket_name,
                              replace=True)
            context["ti"].xcom_push(key="output_s3_key", value=output_s3_key)
        else:
            self.transform_callable(upstream_input_paths=upstream_input_paths,
                                    output_path=None,
                                    **context)
