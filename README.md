# simple_etl_process

**Basic ETL Pipeline**  
A Python-based ETL pipeline to extract, transform, and load data from various file formats (CSV, JSON, XML) into a unified CSV output.

## Overview
This project demonstrates a basic ETL (Extract, Transform, Load) process designed to:
1. **Extract** data from multiple file formats.
2. **Transform** the data, including unit conversions.
3. **Load** the processed data into a single CSV file for consolidated analysis.

## Features
- **Extraction** from CSV, JSON, and XML files located in a specific directory (`../raw`).
- **Transformation** to convert height from inches to meters and weight from pounds to kilograms.
- **Logging** of each stage of the process (Extraction, Transformation, Loading) to provide insights into the ETL execution.
- **Output** of the transformed data to a CSV file located at `../output/transformed_data.csv`.

## Project Structure
- `extract_from_csv(file_to_process)`: Reads and returns data from a CSV file as a DataFrame.
- `extract_from_json(file_to_process)`: Reads and returns data from a JSON file as a DataFrame.
- `extract_from_xml(file_to_process)`: Parses XML, extracts specific fields, and returns the data as a DataFrame.
- `extract()`: Aggregates data from all available files in the `raw` folder.
- `transform(data)`: Converts height and weight units for consistency.
- `load_data(target_file, transformed_data)`: Saves the transformed data to a CSV file.
- `log_progress(message)`: Logs process stages and any errors encountered.
- `is_file_empty(file_path)`: Checks if a file is empty before processing.

## Usage
1. **Place raw files** in the `../raw` directory in either CSV, JSON, or XML format.
2. **Run the ETL process** to execute all three phases (Extraction, Transformation, and Loading).
3. **Check logs** in `../logs/log_file.txt` for detailed information about each process step.

## Requirements
- `pandas`
- `pathlib`
- `xml.etree.ElementTree`

Install required packages using:
```bash
pip install pandas
