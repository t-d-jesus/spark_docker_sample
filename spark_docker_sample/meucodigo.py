from time import sleep
from pathlib import Path
from openweathermap_api.city_list import get_cities
from openweathermap_api.openweathermap_api import extract_data_from_api
import sys
import json

if __name__ == '__main__':

    step = sys.argv[1] if len(sys.argv) > 1 else ''
    # # Cria a sessão do Spark
    # spark = SparkSession.builder \
    #     .appName("MeuApp") \
    #     .master("spark://spark-master:7077") \
    #     .getOrCreate()

    # spark.sparkContext.setLogLevel('INFO')

    # LOGGER = get_logger()


    # LOGGER.info("# Cria um dataframe a partir de uma lista de tuplas#")
    # data = [("User1", 1), ("User2", 2), ("User2", 3), ("User2", 3), ("User4", 4)]
    # df:DataFrame = spark.createDataFrame(data, ["Nome", "Idade"])

    # LOGGER.info("# Exibe o dataframe")
    # df.show()

    # LOGGER.info("# Grava Dataframe")
    # df.write.mode('overwrite').csv('/data/saida.csv')


    # sleep(10)

    # spark.stop()
    cities = None
    with open('resources/city_list.json','r') as json_file:
      cities= json.load(json_file)


    # cities = ['Vue']
    for city in cities:
        print('Extract Data')
        print('Executing extract_data_from_api')
        extract_data_from_api(id=cities[city]['id'],name=city) 
    # print('Process Data')
        # calculate_wheatherBy_city(city)


        # TODO remover condição abaixo
        # if city == 'londres':
        #     break

    # Salva o dataframe em um arquivo CSV
    #df.write.csv("/app/data/meudataframe.csv", header=True, mode="overwrite")

