from spark_docker_sample.transform.transform_weather import highest_weather_by_city, avg_min_max_weather_by_city
import pytest
from pytest import fixture
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Row


@fixture
def spark() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()
    yield spark

    spark.stop()


WHEATHERS_DATA = [
        [6434558, 'Vertou', 47.1667, -1.4833, 1680998400, '2023-04-09 00:00:00', 280.65, 284.38],
        [6434558, 'Vertou', 47.1667, -1.4833, 1681009200, '2023-04-09 03:00:00', 279.57,  281.8],
        [6434558, 'Vertou', 47.1667, -1.4833, 1681020000, '2023-04-09 06:00:00', 279.42, 279.42],
        [6434558, 'Vertou', 47.1667, -1.4833, 1681030800, '2023-04-09 09:00:00', 285.91, 285.91],
        [6434558, 'Vertou', 47.1667, -1.4833, 1681041600, '2023-04-09 12:00:00', 289.87, 289.87]
]

COLUMNS = [
    'id',
    'name',
    'lat',
    'lon',
    'dt',
    'dt_txt',
    'temp_min',
    'temp_max'
]


def test_should_have_highest_temp_max_wheather(spark:SparkSession):

    df_wheathers = spark.createDataFrame(WHEATHERS_DATA,COLUMNS)
    df_highest = highest_weather_by_city(df_wheathers)
    calculated_wheather = df_highest.collect()
    
    expected_wheather = [Row(id=6434558, name='Vertou', lat=47.1667, lon=-1.4833, dt=1681041600, dt_txt='2023-04-09 12:00:00', temp_min=289.87, temp_max=289.87, rank=1)]

    assert calculated_wheather == expected_wheather
