import logging
import pandas as pd
from pytrends.request import TrendReq
import os
import time

import config

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Função principal para coletar dados de tendências com base em uma categoria escolhida
def fetch_trends_by_category(category, products):
    logger.info(f"Extraindo dados de tendências para a categoria: {category}")

    # Configuração da API do Google Trends
    pytrends = TrendReq(hl='pt-BR', tz=360)
    timeframe = 'now 7-d'  # Última semana

    # Coleta de dados de tendências para os produtos
    trends_data = []
    for product in products:
        logger.info(f"Coletando dados para o produto: {product}")
        pytrends.build_payload([product], timeframe=timeframe, geo='BR')
        interest_data = pytrends.interest_over_time()

        if not interest_data.empty:
            avg_interest = round(interest_data[product].mean())
            trends_data.append([product, avg_interest])
            logger.info(f"Produto: {product}, Média de Interesse: {avg_interest}")
        else:
            trends_data.append([product, 0])
            logger.warning(f"Sem dados para o produto: {product}")

    # Criação do DataFrame e salvamento em TSV
    df = pd.DataFrame(trends_data, columns=["Produto", "Média de Interesse"])
    filename = f"{category.replace(' ', '_').lower()}.tsv"
    df.to_csv(filename, sep='\t', index=False)
    logger.info(f"Dados de tendências salvos em {filename}")


def fetch_trends_by_category2(category, products, output_folder=config.TRENDING_PRODUCTS_FOLDER_IN):
    os.makedirs(output_folder, exist_ok=True)
    logger.info(f"Extraindo dados de tendências para a categoria: {category}")

    # Configuração da API do Google Trends
    pytrends = TrendReq(hl='pt-BR', tz=360)
    timeframe = 'now 7-d'  # Última semana

    # Coleta de dados de tendências para os produtos
    trends_data = []
    for product in products:
        logger.info(f"Coletando dados para o produto: {product}")
        pytrends.build_payload([product], timeframe=timeframe, geo='BR')
        interest_data = pytrends.interest_over_time()

        if not interest_data.empty:
            avg_interest = round(interest_data[product].mean())
            trends_data.append([product, avg_interest])
            logger.info(f"Produto: {product}, Média de Interesse: {avg_interest}")
        else:
            trends_data.append([product, 0])
            logger.warning(f"Sem dados para o produto: {product}")

    # Salve o DataFrame no arquivo TSV

    filename = os.path.join(output_folder, f"{category.replace(' ', '_').lower()}.tsv")
    df = pd.DataFrame(trends_data, columns=["Produto", "Média de Interesse"])
    df.to_csv(filename, sep='\t', index=False)
    logger.info(f"Dados de tendências salvos em {filename}")


def execute_getproducts_trends():
    categorias = [
        "telefonia",
        "televisores",
        "eletroportateis",
        "eletrodomesticos",
        "mobiliario"
    ]

    telefonia = [
        "Smartphone Android",
        "iPhone",
        "Telefone fixo sem fio",
        "Fone Bluetooth"
    ]

    televisores = [
        "Smart TV 4K",
        "TV OLED 8K",
        "Smart TV Android",
        "TV QLED 120 Hz"
    ]

    eletroportateis = [
        "Aspirador sem fio",
        "Liquidificador",
        "Airfryer",
        "Batedeira"
    ]

    eletrodomesticos = [
        "Geladeira duplex",
        "Lava e seca",
        "Fogão 5 bocas",
        "Micro-ondas com grill"
    ]

    mobiliario = [
        "Sofá retrátil",
        "Mesa de jantar",
        "Estante",
        "Guarda-roupa"
    ]

    produtos_por_categoria = {
        "telefonia": telefonia,
        "televisores": televisores,
        "eletroportateis": eletroportateis,
        "eletrodomesticos": eletrodomesticos,
        "mobiliario": mobiliario
    }

    for categoria in categorias:
        fetch_trends_by_category2(categoria, produtos_por_categoria[categoria])
        time.sleep(90)


if __name__ == "__main__":
    execute_getproducts_trends()