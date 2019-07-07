# Airflow plugin - LuigiOperator

This plugin includes a single operator that makes it easier to pass intermediary files between tasks on S3.

## Installation

```bash
pip install git+https://github.com/gusostow/airflow-luigi-plugin
```

After installation, the operator is available like this
```python
from airflow.operators.luigi_plugin import LuigiOperator
```
