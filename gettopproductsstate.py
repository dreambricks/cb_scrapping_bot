import pandas as pd
from pytrends.request import TrendReq
import logging
import time
import os

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
arquivo_progresso = "progresso_estado.txt"


# Função para obter as tendências de produtos em um estado específico, pesquisando uma palavra por vez
def obter_tendencias_estado(estado):
    logger.info(f"Iniciando consulta de tendências para o estado: {estado}")

    resultados = []

    # Consultar cada produto individualmente
    for index, produto in enumerate(produtos_populares):
        logger.info(f"Consultando produto: {produto}")

        try:
            pytrends.build_payload([produto], timeframe='now 7-d', geo=f'BR-{estado}')
            interesse_regiao = pytrends.interest_by_region(resolution='REGION')

            if not interesse_regiao.empty and estado in interesse_regiao.index:
                # Extrair apenas a pontuação do estado
                pontuacao = interesse_regiao.loc[estado, produto]
                resultados.append({
                    'Produto': produto,
                    'Pontuação': round(pontuacao),
                    'Estado': estado
                })

        except Exception as e:
            logger.error(f"Erro ao consultar produto {produto}: {e}")

        # Insira uma pausa a cada 5 consultas
        if index % 5 == 0 and index != 0:
            print("Esperando para evitar limite de requisições...")
            time.sleep(60)  # Aguarde 60 segundos

    # Convertendo os resultados em DataFrame
    if resultados:
        resultado_final = pd.DataFrame(resultados)
        return resultado_final
    else:
        logger.warning(f"Nenhum dado encontrado para o estado {estado}")
        return None


# Função para salvar o estado atual em um arquivo
def salvar_progresso(estado):
    with open(arquivo_progresso, "w") as f:
        f.write(estado)
    logger.info(f"Progresso salvo: último estado processado foi {estado}")


def execute_gettoproductsstate():
    global estados

    # Verificar se existe um progresso salvo
    if os.path.exists(arquivo_progresso):
        with open(arquivo_progresso, "r") as f:
            ultimo_estado = f.read().strip()
        # Retomar a partir do próximo estado
        index_ultimo_estado = estados.index(ultimo_estado)
        estados = estados[index_ultimo_estado + 1:]
        logger.info(f"Retomando a execução a partir do estado: {ultimo_estado}")
    else:
        logger.info("Iniciando a execução do zero.")

    # Processar cada estado
    for estado_selecionado in estados:
        if estado_selecionado in estados:
            tendencias_estado = obter_tendencias_estado(estado_selecionado)

            if tendencias_estado is not None:
                tsv_filename = f"produtos_populares_{estado_selecionado.lower()}.tsv"
                tendencias_estado.to_csv(tsv_filename, sep='\t', index=False)
                logger.info(f"Tendências salvas em {tsv_filename}.")
            else:
                logger.warning("Não foi possível obter tendências para o estado selecionado.")

            # Salvar o progresso após cada estado
            salvar_progresso(estado_selecionado)

        else:
            logger.error("Estado inválido. Execute o programa novamente e escolha um estado válido.")

    # Limpar o arquivo de progresso ao finalizar
    if os.path.exists(arquivo_progresso):
        os.remove(arquivo_progresso)


# Função principal para execução do código
if __name__ == "__main__":
    execute_gettoproductsstate()
