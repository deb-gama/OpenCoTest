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
    """
    This method calculates the average rate weighted by the contract value
    """

    values = get_column_values(CSV_FILE, contract_values)
    weight_values = get_column_values(CSV_FILE, rate_weights)

    float_contract_values = convert_to_float(values)
    float_weights = convert_to_float(weight_values)

    # cálculo de taxa média ponderada pelo valor do contrato, arredondada e formatada para representar percentual
    weighted = (
        float_contract_values * (float_weights / 100)
    ).sum() / float_contract_values.sum()
    formated_weighted = round((weighted * 100), 4)

    return formated_weighted


def get_term_weighted_average(contract_values, term_weights):
    """
    This method calculates the average term weighted by the contract value
    """
    values = get_column_values(CSV_FILE, contract_values)
    weight_values = get_column_values(CSV_FILE, term_weights)

    float_contract_values = convert_to_float(values)
    float_weights = convert_to_float(weight_values)

    # cálculo de prazo média ponderada pelo valor do contrato arredondada e formatada
    weighted = (
        float_contract_values * float_weights
    ).sum() / float_contract_values.sum()
    formated_weighted = round(weighted, 2)

    return formated_weighted


def get_bad_values(file_name):
    """
    This method returns a list with the values ​​1 for bad = True and 0 for bad = False
    """
    # obtendo a coluna de atrasos
    delay = get_column_values(file_name, "atraso_corrente")
    delay_int_values = delay.astype(int)
    # atribuindo os vaores de bad sendo 1 para valores de atraso superior a 180 e 0 para inferiores.
    bad_values = np.where(delay_int_values > 180, 1, 0)

    bad_values_list = bad_values.tolist()

    return bad_values_list


def get_bad_values_by_loss(file_name):
    """
    This method returns a list with the values ​​1 for bad = True and 0 for bad = False, considering the loss formula
    """
    # criando dataframe para manipular os dados - eliminando colunas nulas
    data_frame = pd.read_csv(file_name, sep=";").dropna(how="all")

    # dataframe com valores de contratos com atraso superior a 180 dias
    bad_payers_df = data_frame[data_frame["atraso_corrente"] > 180]

    # formatando valores de contrato com juros para usar no cálculo de loss
    formated_interest_values = convert_to_float(
        bad_payers_df["valor_contrato_mais_juros"]
    )
    # obtendo e formatando valores em aberto
    formated_open_value = convert_to_float(bad_payers_df["valor_em_aberto"])

    # aplicando a fórmula e transformando em 1 e 0 (arredondando pra cima e/ou pra baixo) - bad como base
    loss = (formated_open_value / formated_interest_values).round().astype(int)
    loss_list = loss.tolist()

    return loss_list


def get_new_score_dataframe():
    """
    Creates a score from 0 to 1000 considering outstanding debts, losses, and delays
    """
    # definindo os pesos
    delay_weight = 0.55
    debts_weight = 0.12
    loss_weight = 3.3

    # criando dataframe do arquivo para manipular os dados
    dataframe = pd.read_csv(CSV_FILE, sep=";").dropna(how="all")

    # calculando loss para criar coluna nova no dataframe e comparr resultados finais
    open_values = convert_to_float(dataframe["valor_em_aberto"])
    interest_values = convert_to_float(dataframe["valor_contrato_mais_juros"])
    loss = (open_values / interest_values).round().astype(int)
    dataframe["loss"] = np.where(dataframe["atraso_corrente"] > 180, loss, 0)

    # obtendo os valores da tabela para calcular o score
    delay_data = dataframe["atraso_corrente"].astype(float)
    debts_data = dataframe["divida_total_pj"].astype(float)
    loss_data = dataframe["loss"]

    # Calculando o peso dos valores
    weighted_delay = delay_data * delay_weight
    debts_score = debts_data * debts_weight
    loss_score = loss_data * loss_weight

    # Somando o score total
    total_score = weighted_delay + debts_score + loss_score

    # Aplicando normalização para criar score de 0 a 1000. Foi necessário usar lambda após erro de Series value
    normalized_score = total_score.apply(lambda x: min(1000, max(0, x)))
    dataframe["new_score"] = normalized_score.astype(int)

    # criando daataframe com valores que possibilitem a comparação dos resultados obtidos
    dataframe["bad"] = get_bad_values(CSV_FILE)
    selected_columns = [
        "faturamento_informado",
        "divida_total_pj",
        "atraso_corrente",
        "score",
        "bad",
        "loss",
        "new_score",
    ]
    result_df = dataframe[selected_columns]

    return result_df


# DESCOMENTE e rode make run para ver os logs
# ticket = get_average_contract_ticket("openco_etapa1_dataset.csv", "valor_contrato")
# term = get_term_weighted_average("valor_contrato", "prazo")
# rate = get_rate_weighted_average("valor_contrato", "taxa")

# bad_list = get_bad_values("openco_etapa1_dataset.csv")
# bad_by_loss_list = get_bad_values_by_loss("openco_etapa1_dataset.csv")

# print(f"Ticket médio: {ticket}")
# print(f"Prazo médio: {term}")
# print(f"Taxa média: {rate}")

# print(f"Bad: {bad_list.count(1)} - Not bad: {bad_list.count(0)}")
# print(
#     f"Bad - loss: {bad_by_loss_list.count(1)} - Not bad - loss: {bad_by_loss_list.count(0)}"
# )

# print(get_new_score_dataframe())
