import requests
import json
import requests
import datetime
from pathlib import Path
import logging



def _save_api_response_to_json(data, filename):
    """
    Retrieves data from an API and saves it to a JSON file.
    
    Parameters:
        - url: string containing the URL of the API endpoint.
        - filename: string containing the filename of the JSON file to be created.
    """
    print(f'====={filename}======')  


    with open(filename, 'w') as outfile:

        json.dump(data, outfile)


def _get_forecast5days_from_api(url,params):
    response = requests.get(url=url, params=params)

    # Verifica se a requisição foi bem-sucedida
    if response.status_code == 200:
        # Converte o conteúdo da resposta para um objeto JSON
        return response.json()
    else: raise ValueError(f'It Was not possible to request data from the api: Status code returned {response.status_code}')


def extract_data_from_api(id,name):

    current_date = datetime.datetime.now()
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")

    # logging.basicConfig(level=logging.INFO) #Memory
    base = Path(f"data/log/{year}/{month}/{day}")
    base.mkdir(exist_ok=True,parents=True)

    logging.basicConfig(level=logging.INFO, filename=f"data/log/{year}/{month}/{day}/openweathermap_extract_{year}{month}{day}.log", format="%(asctime)s - %(levelname)s - %(message)s")
    

    #TODO Colocar a API_KEY PARA PEGAR COMO VARIAVEL DE AMBIENTE MAS NÃAAOOOOO COMITAR ELA  
    # URL da API
    URL = "https://api.openweathermap.org/data/2.5/forecast"
    API_KEY = "kjafklasjfkjsakfsjfsakjjadsf"
    # Parâmetros da requisição
    params = {
        "id": id,
        "appid": API_KEY
    }

    logging.info(f"Start Reading Wheather data from {URL} for city {name}")

    # Faz a requisição à API
    logging.info(f"Execute requesto to the API")
    data = _get_forecast5days_from_api(URL,params)

    date_string = datetime.datetime.now().strftime("%Y-%m-%d")
    
    base = Path(f"data/extract/{year}/{month}/{day}")
    base.mkdir(exist_ok=True,parents=True)
    

    filename = f"{base}/{name}_{date_string}.json"

    logging.info(f"Save the data requested from the API")
    _save_api_response_to_json(data, filename)

    logging.info(f"Finish extraction for city {name}")

