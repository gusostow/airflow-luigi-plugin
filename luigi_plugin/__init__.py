from airflow.plugins_manager import AirflowPlugin

from luigi_plugin.operators import luigi_operator

__version__ = '0.1.0'


class LuigiPlugin(AirflowPlugin):
    name = 'luigi_plugin'
    operators = [luigi_operator.LuigiOperator]
    hooks = []
