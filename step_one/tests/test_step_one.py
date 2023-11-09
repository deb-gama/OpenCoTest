import os
import unittest
import pytest
import tempfile
import pandas as pd
from unittest.mock import patch, mock_open
from step_one.main import (
    get_column_values,
    get_average_contract_ticket,
    get_rate_weighted_average,
    convert_to_float,
    get_term_weighted_average,
    get_bad_values,
    get_bad_values_by_loss,
)


mocked_data = pd.DataFrame.from_dict(
    {
        "taxa": ["4,87", "5", "4,3", "4,4"],
        "atraso_corrente": ["332", "0", "30", "180"],
        "prazo": ["6", "12,6", "10", "6"],
        "valor_contrato": ["1800,2514", "4500,3356", "400,0023", "700,1245"],
        "valor_contrato_mais_juros": ["2200", "8000", "600", "900"],
        "valor_em_aberto": ["1300", "0", "0", "0"],
    }
)


@patch("builtins.open", new_callable=mock_open, read_data=mocked_data.to_csv(sep=";"))
class StepOneTests(unittest.TestCase):
    def test_get_column_values_must_return_the_values_of_a_specific_column(
        self, mock_file
    ):
        csv_file_path = "fake_path_to_csv.csv"
        response = get_column_values(csv_file_path, "prazo")

        self.assertEqual(response.tolist(), ["6", "12,6", "10", "6"])

    def test_get_average_contracts_ticket(self, mock_file):
        csv_file_path = "fake_path_to_csv.csv"
        response = get_average_contract_ticket(csv_file_path, "valor_contrato")

        self.assertEqual(response, 1850.17845)

    def test_convert_to_float_method_must_receive_strings_and_return_float_numbers(
        self, mock_file
    ):
        csv_file_path = "fake_path_to_csv.csv"
        rate_values = get_column_values(csv_file_path, "taxa")
        response = convert_to_float(rate_values)

        self.assertEqual(response.tolist(), [4.87, 5, 4.3, 4.4])

    def test_rate_weighted_average(self, mock_file):
        response = get_rate_weighted_average("valor_contrato", "taxa")

        self.assertEqual(response, 4.8738)

    def test_get_term_weighted_average(self, mock_file):
        response = get_term_weighted_average("valor_contrato", "prazo")

        self.assertEqual(response, 10.23)

    def test_get_bad_values_method_must_return_the_quantity_of_good_and_bad_payers(
        self, mock_file
    ):
        csv_file_path = "fake_path_to_csv.csv"
        bad_payers_list = get_bad_values(csv_file_path)

        self.assertEqual(bad_payers_list.count(1), 1)
        self.assertEqual(bad_payers_list.count(0), 3)
