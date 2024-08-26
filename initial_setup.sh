sudo apt install virtualenv
virtualenv -p python3.10 .venv
pip install -r requirements.txt

# Airflow installation
export AIRFLOW_HOME=~/airflow
export AIRFLOW_VERSION=2.10.0

# Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
# See above for supported versions.
export PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 2.10.0 with python 3.8: https://raw.githubusercontent.com/apache/airflow/constraints-2.10.0/constraints-3.8.txt

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
