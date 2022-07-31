from airflow.decorators import task

@task.python
def load_data():
    print("Loading data...")
    return "Loaded data!"

@task.python
def transform_data():
    print("Transforming data...")
    return "Transformed data!"

@task.python
def extract_data():
    print("Extracting data...")
    return "Extracted data!"
