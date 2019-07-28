from setuptools import setup, find_packages


with open('requirements.txt', 'r') as f:
    install_requires = f.read().splitlines()

setup(
    name='luigi_plugin',
    version='0.1',
    install_requires=install_requires,
    entry_points={"airflow.plugins": ["luigi_plugin = luigi_plugin:LuigiPlugin"]},
    packages=find_packages(),
)
