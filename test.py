import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import DataFrame
from datetime import datetime
# Adjust the import path according to your project structure
from AA_LTR.src.build.base_break_trxns_wire_slctn import BaseBreakTrxnsWireSlctn

class TestBaseBreakTrxnsWireSlctn(unittest.TestCase):
    @patch('AA_LTR.src.build.utils.common.get_date_list')
    @patch('AA_LTR.src.build.utils.common.write_parquet_to_s3')
    @patch('AA_LTR.src.build.config.Config')
    @patch('AA_LTR.src.build.utils.dataset_loader.getSpark')
    def test_generate(self, mock_getSpark, mock_Config, mock_write_parquet_to_s3, mock_get_date_list):
        # Setup mock return values
        mock_Config.return_value.LOOKBACK_7 = 7
        mock_Config.return_value.REPO_DATA_DIR = '/fake/path/to/data'
        mock_get_date_list.return_value = ['2021-01-01']
        
        mock_spark_session = MagicMock()
        mock_getSpark.return_value = mock_spark_session
        mock_spark_session.sql.return_value = MagicMock(spec=DataFrame)
        
        mock_df = MagicMock(spec=DataFrame)
        # Mocking the DataFrame operations
        mock_spark_session.read.format.return_value.option.return_value.load.return_value = mock_df
        
        # Instantiate the class to be tested
        wire_selection = BaseBreakTrxnsWireSlctn()
        
        # Mocking internal methods to bypass actual implementation
        wire_selection.get_break_selection_accts = MagicMock(return_value=mock_df)
        wire_selection.get_rpt_wire_trxns = MagicMock(return_value=mock_df)
        wire_selection.get_wire_trxns_for_all_accts = MagicMock(return_value=mock_df)
        
        # Adjust the test to pass a date string in the required format
        run_date_str = datetime.now().strftime('%Y-%m-%d')

        # Execute the method under test with the correctly formatted date string
        wire_selection.generate(run_date_str)

        # Assert that external dependencies are called as expected
        mock_get_date_list.assert_called_once()
        mock_write_parquet_to_s3.assert_called_once()
        wire_selection.get_break_selection_accts.assert_called_once()
        wire_selection.get_rpt_wire_trxns.assert_called_once()
        wire_selection.get_wire_trxns_for_all_accts.assert_called_once()

if __name__ == '__main__':
    unittest.main()
