from setuptools import setup, find_packages

setup(
    name="dataforge",
    version="1.0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas>=2.0.0",
        "openpyxl>=3.1.0",
        "pyyaml>=6.0",
        "python-dotenv>=1.0.0",
        "requests>=2.31.0",
        "sqlalchemy>=2.0.0",
        "psycopg2-binary>=2.9.9",
        "redis>=5.0.0",
        "boto3>=1.34.0",
        "pydantic>=2.5.0",
        "pydantic-settings>=2.1.0",
        "tenacity>=8.2.0",
        "python-dateutil>=2.8.0",
        "pytz>=2023.3",
        "streamlit>=1.45.0",
    ],
    entry_points={
        "console_scripts": [
            "dataforge=dataforge.cli:main",
        ],
    },
    python_requires=">=3.11",
)
