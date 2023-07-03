# Telecommunication Billing Data Pipeline Unit Tests
This documentation provides an overview of the unit tests developed for a telecommunication billing data pipeline. The data pipeline consists of three functions: data_extraction, data_transformation, and data_loading. The unit tests ensure that these functions handle various scenarios and edge cases correctly.

## Setup
The unit tests are implemented using the unittest framework in Python. The tests are written in a test case class called TestDataPipeline, which subclasses unittest.TestCase. The sample input dataset is provided as a dictionary input_data, representing the telecommunication billing data. The input data is written to a CSV file, which serves as the input for the data pipeline functions. The output of the data pipeline is also written to a CSV file for verification.

## Functions
The following functions are tested in the data pipeline:

data_extraction(file_path): This function reads the telecommunication billing data from a CSV file using pandas and returns a DataFrame containing the data.

data_transformation(data): This function performs transformations on the input data. It drops any duplicate rows, converts the billing_amount column from a string format to a float, and computes the total_charges column by adding the billing_amount and tax_amount columns. The transformed data is returned as a DataFrame.

data_loading(data, output_file): This function takes the transformed data DataFrame and writes it to a CSV file.

## Unit Tests
The unit tests cover different scenarios and edge cases for each function in the data pipeline. Here's an overview of the tests for each function:

### data_extraction
test_data_extraction: This test verifies that the data_extraction function correctly reads the CSV file and returns a DataFrame. It checks the type of the returned object, the number of rows in the DataFrame, and the column names.

test_data_extraction_empty_file: This test covers the edge case where the input file is empty. It checks that an empty DataFrame is returned.

### data_transformation
test_data_transformation: This test validates the correctness of the data_transformation function. It verifies that the output DataFrame has the expected columns, the correct data types for billing_amount and tax_amount, and computes the total_charges column correctly.

test_data_transformation_missing_columns: This test handles the edge case where the input DataFrame has missing columns. It ensures that an empty DataFrame is returned.

### data_loading
test_data_loading: This test checks the data_loading function's ability to write the transformed data DataFrame to a CSV file. It verifies the existence of the output file and compares the loaded data from the file with the transformed data DataFrame.

test_data_loading_empty_data: This test covers the edge case where the input data is empty. It checks that the output file is created but contains no data.

## Running the Tests
To run the unit tests, execute the script using a Python interpreter. The unittest module will discover the test cases and run them. Each test method will be executed, and assertions within the tests will be checked for correctness. If all assertions pass, the tests will succeed, indicating that the data pipeline functions are working as expected. Any failed assertions will be reported, providing informative error messages for debugging.

It's important to note that the tests are designed to be independent and do not rely on each other. The tearDown method is used to clean up any files created during the tests, ensuring a clean state for each test run.

Overall, the unit tests provide confidence in the correctness of the telecommunication billing data pipeline by covering different
