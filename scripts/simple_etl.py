import glob

import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "../logs/log_file.txt"
target_file = "../output/transformed_data.csv"


def extract_from_csv(file_to_process):
    try:
        dataframe = pd.read_csv(file_to_process)
        return dataframe
    except Exception as e:
        log_progress(f"Error reading CSV file {file_to_process}: {e}")
        return pd.DataFrame()



def extract_from_json(file_to_process):
    try:
        dataframe = pd.read_json(file_to_process, lines=True)
        return dataframe
    except Exception as e:
        log_progress(f"Error reading JSON file {file_to_process}: {e}")
        return pd.DataFrame()


def extract_from_xml(file_to_process):
    try:
        data = []
        tree = ET.parse(file_to_process)
        root = tree.getroot()

        for person in root:
            name = person.find("name").text
            height = float(person.find("height").text)
            weight = float(person.find("weight").text)
            data.append({"name": name, "height": height, "weight": weight})

        dataframe = pd.DataFrame(data, columns=["name", "height", "weight"])
        return dataframe
    except Exception as e:
        log_progress(f"Error reading XML file {file_to_process}: {e}")
        return pd.DataFrame()



def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])

    for csvfile in glob.glob("../raw/*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

    for jsonfile in glob.glob("../raw/*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    for xmlfile in glob.glob("../raw/*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extracted_data

# Convert height(inches) and weight(pounds) into meters and kilograms
def transform(data):
    data['height'] = round(data.height * 0.0254, 2)
    data['weight'] = round(data.weight * 0.45359237, 2)

    return data


def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')



# Log the initialization of the ETL process
log_progress("ETL Job Started")

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
extracted_data = extract()

# Log the completion of the Extraction process
log_progress("Extract phase Ended")

# Log the beginning of the Transformation process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Loading process
log_progress("Load phase Started")
load_data(target_file, transformed_data)

# Log the completion of the Loading process
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")