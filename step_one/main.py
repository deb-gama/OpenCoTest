import numpy as np
import pandas as pd
from statistics import mean

CSV_FILE = "openco_etapa1_dataset.csv"


def get_column_values(file_name, column_name):
    """
    This method reads a csv file separated by semicolon, discard NaN values, and return valid values
    of a column received by parameter.
    """
    data = pd.read_csv(file_name, sep=";").dropna(how="all")
    data_values = data[column_name]

    return data_values


def convert_to_float(values):
    """
    This method receives a list of comma-separated values, replaces the commas with
    periods and converts them to float type.
    """
    float_values = values.str.replace(",", ".").astype(float)

    return float_values


def get_average_contract_ticket(file_name, contracts_values_column):
    """
    This method converts contract values into float and returns the average that
    corresponds to the average contract ticket in the sample.
    """
    values = get_column_values(file_name, contracts_values_column)
    formated_values = convert_to_float(values)
    average = formated_values.mean()

    return average


def get_rate_weighted_average(contract_values, rate_weights):
    values = get_column_values(CSV_FILE, contract_values)
    weight_values = get_column_values(CSV_FILE, rate_weights)

    float_contract_values = convert_to_float(values)
    float_weights = convert_to_float(weight_values)

    weighted = (
        float_contract_values * (float_weights / 100)
    ).sum() / float_contract_values.sum()
    formated_weighted = round((weighted * 100), 4)

    return formated_weighted


def get_term_weighted_average(contract_values, term_weights):
    values = get_column_values(CSV_FILE, contract_values)
    weight_values = get_column_values(CSV_FILE, term_weights)

    float_contract_values = convert_to_float(values)
    float_weights = convert_to_float(weight_values)

    weighted = (
        float_contract_values * float_weights
    ).sum() / float_contract_values.sum()
    formated_weighted = round(weighted, 2)

    return formated_weighted


def get_bad_values(file_name):
    delay = get_column_values(file_name, "atraso_corrente")
    delay_int_values = delay.astype(int)

    bad_values = np.where(delay_int_values > 180, 1, 0)

    is_bad = bad_values.tolist().count(1)
    is_not_bad = bad_values.tolist().count(0)

    return is_bad, is_not_bad


def get_bad_values_by_loss(file_name):
    data_frame = pd.read_csv(file_name, sep=";").dropna(how="all")

    # dataframe com valores de contratos com atraso superior a 180 dias
    bad_payers_df = data_frame[data_frame["atraso_corrente"] > 180]
    formated_interest_values = convert_to_float(
        bad_payers_df["valor_contrato_mais_juros"]
    )
    formated_open_value = convert_to_float(bad_payers_df["valor_em_aberto"])

    loss = (formated_open_value / formated_interest_values).round().astype(int)
    # transformar em 1 e 0 e comparar (arredondando pra cima e/ou pra baixo)

    bad_by_loss = loss.tolist().count(1)
    not_bad_by_loss = loss.tolist().count(0)

    return bad_by_loss, not_bad_by_loss


ticket = get_average_contract_ticket("openco_etapa1_dataset.csv", "valor_contrato")
term = get_term_weighted_average("valor_contrato", "prazo")
rate = get_rate_weighted_average("valor_contrato", "taxa")

bad, not_bad = get_bad_values("openco_etapa1_dataset.csv")
bad_by_loss, not_bad_by_loss = get_bad_values_by_loss("openco_etapa1_dataset.csv")

print(ticket)
print(term)
print(rate)

print(f"Bad: {bad} - Not bad: {not_bad}")
print(f"Bad - loss: {bad_by_loss} - Not bad - loss: {not_bad_by_loss}")
