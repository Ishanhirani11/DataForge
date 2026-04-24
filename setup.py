"""
Setup script for DataFlow Pro.
"""

from setuptools import setup, find_packages

if __name__ == "__main__":
    setup(
        name="dataflow-pro",
        version="0.1.0",
        description="Professional Data Engineering Framework",
        author="DataFlow Pro Team",
        author_email="dataflow-pro@example.com",
        python_requires=">=3.11",
        packages=find_packages(where="src"),
        package_dir={"": "src"},
        py_modules=[],
        install_requires=[
            "requests>=2.31.0",
            "sqlalchemy>=2.0.0",
            "pandas>=2.1.0",
            "pyyaml>=6.0.0",
        ],
        extras_require={
            "dev": [
                "pytest>=7.4.0",
                "pytest-cov>=4.1.0",
                "ruff>=0.1.0",
                "black>=23.0.0",
                "mypy>=1.6.0",
                "isort>=5.12.0",
            ],
            "test": [
                "pytest>=7.4.0",
                "pytest-cov>=4.1.0",
                "pytest-mock>=3.12.0",
                "pytest-asyncio>=0.21.0",
            ],
            "all": [
                "psycopg2-binary>=2.9.9",
                "redis>=5.0.0",
                "boto3>=1.33.0",
                "pyarrow>=14.0.0",
                "openpyxl>=3.1.0",
            ],
        },
        entry_points={
            "console_scripts": [
                "dataflow-pro=dataflow_pro.cli:main",
            ],
        },
    )