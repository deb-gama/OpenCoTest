from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.master("local").appName("PySpark_01").getOrCreate()


def get_vendors_ids_list(lookup_file):
    df_lookup = spark.read.csv(lookup_file, header=True, inferSchema=True)
    vendors = list(df_lookup.select("vendor_id").toPandas()["vendor_id"])

    return vendors


def get_total_distance_by_vendor(file_json):
    df_json = spark.read.json(file_json).select("vendor_id", "trip_distance")
    vendors_list = get_vendors_ids_list("data-vendor_lookup.csv")

    data = []
    columns = ["vendor_id", "trip_sum"]

    for vendor in vendors_list:
        df_by_vendor = df_json.filter(df_json.vendor_id == vendor)
        trip_sum = df_by_vendor.select(sum("trip_distance"))
        row = [vendor, trip_sum.collect()[0][0]]
        data.append(row)

    df_sum = spark.createDataFrame(data=data, schema=columns)
    df_sum.dropna(how="any").show()


def get_vendor_with_highest_numbers_of_trip_per_year(trips_by_year_file):
    # lendo json e selcionando as colunas que serão utilizadas
    df_json = spark.read.json(trips_by_year_file).select("vendor_id", "trip_distance")

    # lista de vendor_ids a partir do lookup para iterador
    vendors_list = get_vendors_ids_list("data-vendor_lookup.csv")

    # criando dataframe com as quantidades de viagens de cada vendor
    data = []
    columns = ["vendor_id", "trip_qtd"]
    for vendor in vendors_list:
        # para cada vendor_id da lista é criada uma linha com a respectiva quantidade total de viagens
        df_by_vendor = df_json.filter(df_json.vendor_id == vendor)
        trip_qtd = df_by_vendor.filter(col("trip_distance").isNotNull()).count()
        row = [vendor, trip_qtd]
        data.append(row)

    df_total_trips_by_vendor = spark.createDataFrame(data=data, schema=columns)
    # ordenando df pela quantidade de viagens da maior para a menor (descendente), sendo o primeiro valor
    # a resposta desejada: maior quantidade de viagens
    response = df_total_trips_by_vendor.orderBy(col("trip_qtd").desc()).first()

    # vendor que viajou mais - nome
    df_vendors_data = spark.read.csv(
        "data-vendor_lookup.csv", header=True, inferSchema=True
    )
    vendor_name = (
        df_vendors_data.filter(df_vendors_data.vendor_id == response.vendor_id)
        .collect()[0]
        .name
    )

    return vendor_name


def get_week_with_the_most_trips_per_year(trips_of_year_file):
    df_json = spark.read.json(trips_of_year_file)

    df_weeks = df_json.select(
        col("pickup_datetime"), weekofyear(col("pickup_datetime")).alias("weekofyear")
    )

    set_weeks = df_weeks.select(collect_set("weekofyear")).collect()[0][0]

    data = []
    columns = ["week", "trip_qtd"]
    for week in set_weeks:
        df_week = df_weeks.filter(df_weeks.weekofyear == week)
        trip_by_week_qtd = df_week.filter(col("weekofyear").isNotNull()).count()
        row = [week, trip_by_week_qtd]

        data.append(row)

    df_trips_by_week = spark.createDataFrame(data=data, schema=columns)

    response = df_trips_by_week.orderBy(col("trip_qtd").desc()).first()

    # semana = response.week,quantidade de viagens = response.trip_qtd
    return response


def get_trips_by_week(vendor_id):
    dates = [("2009", 11), ("2010", 43), ("2011", 16), ("2012", 29)]

    for index, data in enumerate(dates):
        year = data[0]
        week = data[1]
        df_json = spark.read.json(f"data-nyctaxi-trips-{year}.json").select(
            "vendor_id", "pickup_datetime"
        )

        df_weeks = df_json.select(
            col("vendor_id"), weekofyear(col("pickup_datetime")).alias("weekofyear")
        )

        df_by_vendor = df_weeks.filter(
            (df_weeks.vendor_id == vendor_id) & (df_weeks.weekofyear == week)
        ).count()

        print(f"semana {week} / quantidade de viagens: {df_by_vendor}")


# trips_09 = get_vendor_with_highest_numbers_of_trip_per_year('data-nyctaxi-trips-2009.json')
# trips_10 = get_vendor_with_highest_numbers_of_trip_per_year('data-nyctaxi-trips-2010.json')
# trips_11 = get_vendor_with_highest_numbers_of_trip_per_year('data-nyctaxi-trips-2011.json')
# trips_12 = get_vendor_with_highest_numbers_of_trip_per_year('data-nyctaxi-trips-2012.json')

# print(trips_09)
# print(trips_10)
# print(trips_11)
# print(trips_12)

# trips_per_week_09 = get_week_with_the_most_trips_per_year('data-nyctaxi-trips-2009.json')
# trips_per_week_10 = get_week_with_the_most_trips_per_year('data-nyctaxi-trips-2010.json')
# trips_per_week_11 = get_week_with_the_most_trips_per_year('data-nyctaxi-trips-2011.json')
# trips_per_week_12 = get_week_with_the_most_trips_per_year('data-nyctaxi-trips-2012.json')

# print(trips_per_week_09)
# print(trips_per_week_10)
# print(trips_per_week_11)
# print(trips_per_week_12)

# total_distance_09 = get_total_distance_by_vendor('data-nyctaxi-trips-2009.json')
# total_distance_10 = get_total_distance_by_vendor('data-nyctaxi-trips-2010.json')
# total_distance_11 = get_total_distance_by_vendor('data-nyctaxi-trips-2011.json')
# total_distance_12 = get_total_distance_by_vendor('data-nyctaxi-trips-2012.json')

# get_trips_by_week('CMT')
