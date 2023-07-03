import pandas as pd
import unittest

# Sample input dataset
input_data = {
    'customer_id': [1, 2, 3, 4, 5],
    'billing_amount': ['$100', '$200', '$300', '$400', '$500'],
    'tax_amount': [10, 20, 30, 40, 50]
}

# Path to the input file
file_path = 'billing_data.csv'

# Path to the output file
output_file = 'transformed_data.csv'

def create_input_csv(file_path, data):
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)

class TestDataPipeline(unittest.TestCase):
    def tearDown(self):
        # Clean up the created files after each test
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(output_file):
            os.remove(output_file)

    def test_data_extraction(self):
        create_input_csv(file_path, input_data)

        # Test data_extraction function
        extracted_data = data_extraction(file_path)

        # Check if the extracted_data is a DataFrame
        self.assertIsInstance(extracted_data, pd.DataFrame)

        # Check the number of rows in the extracted_data
        self.assertEqual(len(extracted_data), len(input_data['customer_id']))

        # Check if the extracted_data columns match the input_data columns
        self.assertListEqual(list(extracted_data.columns), list(input_data.keys()))

        # Edge case: Test with an empty file
        create_input_csv(file_path, {})
        extracted_data = data_extraction(file_path)
        self.assertTrue(extracted_data.empty)

    def test_data_transformation(self):
        create_input_csv(file_path, input_data)

        # Test data_transformation function
        extracted_data = data_extraction(file_path)
        transformed_data = data_transformation(extracted_data)

        # Check if the transformed_data is a DataFrame
        self.assertIsInstance(transformed_data, pd.DataFrame)

        # Check if the transformed_data has the expected columns
        expected_columns = ['customer_id', 'billing_amount', 'tax_amount', 'total_charges']
        self.assertListEqual(list(transformed_data.columns), expected_columns)

        # Check if the billing_amount and tax_amount columns are of the correct type
        self.assertIsInstance(transformed_data['billing_amount'].dtype, pd.core.dtypes.dtypes.Float64Dtype)
        self.assertIsInstance(transformed_data['tax_amount'].dtype, pd.core.dtypes.dtypes.Float64Dtype)

        # Check if the total_charges column is computed correctly
        expected_total_charges = transformed_data['billing_amount'] + transformed_data['tax_amount']
        self.assertTrue(transformed_data['total_charges'].equals(expected_total_charges))

        # Edge case: Test with missing columns
        create_input_csv(file_path, {'customer_id': [1, 2, 3]})
        extracted_data = data_extraction(file_path)
        transformed_data = data_transformation(extracted_data)
        self.assertTrue(transformed_data.empty)

    def test_data_loading(self):
        create_input_csv(file_path, input_data)

        # Test data_loading function
        extracted_data = data_extraction(file_path)
        transformed_data = data_transformation(extracted_data)
        data_loading(transformed_data, output_file)

        # Check if the output file exists
        self.assertTrue(os.path.exists(output_file))

        # Read the output file and compare the data with the transformed_data
        loaded_data = pd.read_csv(output_file)

        # Check if the loaded_data is a DataFrame
        self.assertIsInstance(loaded_data, pd.DataFrame)

        # Check if the loaded_data columns match the transformed_data columns
        self.assertListEqual(list(loaded_data.columns), list(transformed_data.columns))

        # Check if the loaded_data values match the transformed_data values
        self.assertTrue(loaded_data.equals(transformed_data))

        # Edge case: Test with empty data
        create_input_csv(file_path, {})
        extracted_data = data_extraction(file_path)
        transformed_data = data_transformation(extracted_data)
        data_loading(transformed_data, output_file)
        loaded_data = pd.read_csv(output_file)
        self.assertTrue(loaded_data.empty)

if __name__ == '__main__':
    unittest.main()
