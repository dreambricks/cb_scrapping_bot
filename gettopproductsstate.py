import pandas as pd
from pytrends.request import TrendReq
import logging
import time
import os
from datetime import datetime

import config

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuração do pytrends
pytrends = TrendReq(hl='pt-BR', tz=360)

# Lista de estados do Brasil
estados = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
           "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
           "RS", "RO", "RR", "SC", "SP", "SE", "TO"]

# Produtos populares de e-commerce para monitoramento
produtos_populares = [
    "Celular", "iPhone", "Samsung Galaxy", "Xiaomi", "Motorola", "Nokia", "OnePlus", "Realme",
    "Smartwatch", "Fone de Ouvido Bluetooth", "Carregador Portátil", "Capinha de Celular",
    "Smart TV", "Televisão LED", "TV 4K", "TV 8K", "TV OLED", "TV QLED", "TV 50 polegadas",
    "Chromecast", "Fire Stick", "Apple TV", "Suporte para TV", "Caixa de Som para TV",
    "Airfryer", "Cafeteira", "Batedeira", "Liquidificador", "Fritadeira Elétrica",
    "Aspirador de Pó", "Ferro de Passar", "Panela Elétrica", "Ventilador", "Purificador de Água",
    "Chaleira Elétrica", "Espremedor de Frutas", "Processador de Alimentos", "Grill Elétrico",
    "Geladeira", "Fogão", "Micro-ondas", "Máquina de Lavar", "Lava e Seca", "Freezer",
    "Aquecedor de Água", "Forno Elétrico", "Adega Climatizada", "Exaustor de Cozinha",
    "Cooktop", "Depurador de Ar", "Climatizador de Ar",
    "Sofá", "Cama Box", "Mesa de Jantar", "Guarda-roupa", "Escrivaninha", "Poltrona",
    "Rack para TV", "Estante", "Cadeira de Escritório", "Colchão", "Painel para TV",
    "Balcão de Cozinha", "Sapateira", "Armário Multiuso", "Penteadeira", "Berço",
    "Mesa de Centro", "Cadeira Gamer", "Banqueta Alta"
]

# Caminho do arquivo de progresso
arquivo_progresso = "progresso_tendencias.txt"
output_folder = config.MAP_TSV_FOLDER_IN
os.makedirs(output_folder, exist_ok=True)
arquivo_resultados = os.path.join(output_folder, "item_mais_procurado_por_regiao.tsv")

# Função para salvar progresso
def salvar_progresso(estado_atual, resultados_parciais):
    with open(arquivo_progresso, "w") as f:
        f.write(estado_atual)
    resultados_parciais.to_csv("resultados_parciais.tsv", sep='\t', index=False)
    logger.info(f"Progresso salvo: {estado_atual}")

# Função para carregar progresso
def carregar_progresso():
    if os.path.exists(arquivo_progresso):
        with open(arquivo_progresso, "r") as f:
            return f.read().strip()
    return None

# Função para carregar resultados existentes
def carregar_resultados_existentes():
    if os.path.exists(arquivo_resultados):
        return pd.read_csv(arquivo_resultados, sep='\t')
    return pd.DataFrame(columns=["Estado", "Item", "LastUpdate"])

# Função para obter o item mais procurado por estado
def obter_item_mais_procurado_por_estado():
    resultados = carregar_resultados_existentes()

    # Carregar progresso anterior
    ultimo_estado = carregar_progresso()
    if ultimo_estado:
        logger.info(f"Retomando a partir do estado: {ultimo_estado}")
        estados_restantes = estados[estados.index(ultimo_estado):]
    else:
        estados_restantes = estados
        logger.info("Iniciando a execução do zero.")

    for estado in estados_restantes:
        logger.info(f"Consultando tendências para o estado: {estado}")

        maior_pontuacao = 0
        item_mais_procurado = None

        for produto in produtos_populares:
            try:
                # Construir o payload para o estado e produto
                pytrends.build_payload([produto], timeframe='now 7-d', geo=f'BR-{estado}')
                interesse_regiao = pytrends.interest_by_region(resolution='REGION')

                if not interesse_regiao.empty and estado in interesse_regiao.index:
                    pontuacao = interesse_regiao.loc[estado, produto]

                    # Atualizar o item mais procurado se encontrar uma pontuação maior
                    if pontuacao > maior_pontuacao:
                        maior_pontuacao = pontuacao
                        item_mais_procurado = produto

            except Exception as e:
                logger.error(f"Erro ao consultar produto {produto} no estado {estado}: {e}")
                continue

        if item_mais_procurado:
            logger.info(f"Estado: {estado} | Item mais procurado: {item_mais_procurado}")
            timestamp_atual = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

            # Atualizar ou adicionar resultado no DataFrame
            if estado in resultados["Estado"].values:
                resultados.loc[resultados["Estado"] == estado, ["Item", "LastUpdate"]] = [item_mais_procurado, timestamp_atual]
            else:
                resultados = pd.concat([resultados, pd.DataFrame([{
                    "Estado": estado,
                    "Item": item_mais_procurado,
                    "LastUpdate": timestamp_atual
                }])], ignore_index=True)

        # Adicionar um pequeno delay entre consultas para evitar bloqueios
        time.sleep(60)

        # Salvar progresso e resultados parciais
        salvar_progresso(estado, resultados)

    return resultados

def execute_getproductsstate():
    logger.info("Iniciando coleta de dados...")
    dados = obter_item_mais_procurado_por_estado()

    if not dados.empty:
        # Salvar o resultado final no arquivo
        dados.to_csv(arquivo_resultados, sep='\t', index=False)
        logger.info(f"Arquivo atualizado: {arquivo_resultados}")

        # Remover arquivos de progresso após a conclusão
        if os.path.exists(arquivo_progresso):
            os.remove(arquivo_progresso)
        if os.path.exists("resultados_parciais.tsv"):
            os.remove("resultados_parciais.tsv")
    else:
        logger.warning("Nenhum dado coletado.")


# Função principal
if __name__ == "__main__":
    execute_getproductsstate()
