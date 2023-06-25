FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows
COPY data /opt/prefect/data

RUN find /opt/prefect/ -name "*.py" -exec sed -i 's|ROOT_DIRECTORY = "d:/.*||' {} \;
RUN find /opt/prefect/ -name "*.py" -exec sed -i 's|local_path = Path.*||' {} \;
RUN find /opt/prefect/ -name "*.py" -exec sed -i 's|local_path|path|' {} \;