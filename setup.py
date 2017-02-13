from setuptools import setup, find_packages

setup(
    name="airflow_imaging_plugins",
    description="Airflow plugins to support Neuroimaging tasks.",
    author='LREN CHUV',
    author_email='ludovic.claude@chuv.ch',
    url='https://github.com/LREN-CHUV/airflow-imaging-plugins',
    license='Apache License 2.0',
    zip_safe=False,
    packages=find_packages(exclude=['docs', 'tests*']),
    install_requires=[
        'airflow>=1.7.0',
        'mri-meta-extract>=1.2.2'
    ]
)
