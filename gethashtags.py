import csv
import time
import logging
import pandas as pd
from pytrends.request import TrendReq
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementClickInterceptedException
import os

import config

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Função para combinar os dados em um único DataFrame e salvar como TSV
def combine_and_save_trends(twitter_data, google_data):
    logger.info("Combinando dados de todas as plataformas...")

    # Convertendo os dados em DataFrames
    twitter_df = pd.DataFrame(twitter_data, columns=["Hashtag", "Contagem"])
    google_df = google_data  # Já é um DataFrame

    # Concatenando os DataFrames
    combined_df = pd.concat([twitter_df, google_df], ignore_index=True)

    # Convertendo a coluna "Contagem" para numérico (se necessário)
    combined_df['Contagem'] = pd.to_numeric(combined_df['Contagem'], errors='coerce').fillna(0)

    # Ordenando por "Contagem" em ordem decrescente
    combined_df = combined_df.sort_values(by="Contagem", ascending=False)

    output_folder = config.WORD_TSV_FOLDER_IN

    os.makedirs(output_folder, exist_ok=True)

    # Salvando em um arquivo TSV
    tsv_filename = os.path.join(output_folder, "all_trends.tsv")
    combined_df.to_csv(tsv_filename, sep='\t', index=False)

    logger.info(f"Dados combinados salvos em {tsv_filename}.")

# Função para extrair tendências do Twitter usando Selenium
def get_twitter_trends(url):
    logger.info("Iniciando extração de tendências do Twitter...")
    service = Service(ChromeDriverManager().install())
    options = Options()
    driver = webdriver.Chrome(service=service, options=options)
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")

    try:
        driver.get(url)
        logger.info("Página do Twitter Trends carregada.")

        # Verifica e fecha possíveis pop-ups sobrepondo a página
        try:
            logger.info("Verificando pop-ups na página...")
            overlay_close_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[@aria-label='Fechar' or contains(@class, 'close')]"))
            )
            overlay_close_button.click()
            logger.info("Pop-up fechado com sucesso.")
        except (TimeoutException, NoSuchElementException):
            logger.info("Nenhum pop-up encontrado.")

        # Usa JavaScript para forçar o clique no botão
        try:
            logger.info("Procurando botão de navegação...")
            button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "tab-link-table"))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", button)
            driver.execute_script("arguments[0].click();", button)
            logger.info("Botão clicado com sucesso.")
        except TimeoutException:
            logger.error("Botão de navegação não encontrado.")
            return []

        # Espera até que os elementos com os tópicos estejam visíveis
        logger.info("Esperando que os tópicos sejam carregados...")
        WebDriverWait(driver, 10).until(
            EC.visibility_of_all_elements_located((By.CSS_SELECTOR, "td.topic a"))
        )

        topic_elements = driver.find_elements(By.CSS_SELECTOR, "td.topic")
        count_elements = driver.find_elements(By.CSS_SELECTOR, "td.count")

        trends = []
        for count, topic in zip(count_elements, topic_elements):
            formatted_count = count.text.replace('.', '').replace(',', '')  # Remove pontos e vírgulas
            trends.append([topic.text, formatted_count])

        logger.info(f"Tendências extraídas: {len(trends)} itens encontrados.")

        # Salvar em CSV com tabulação
        csv_filename = "twitter_trends.tsv"
        with open(csv_filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerow(["Hashtags", "Contagem"])
            writer.writerows(trends)

        logger.info(f"Tendências salvas em {csv_filename}.")
        return trends if trends else []

    finally:
        logger.info("Fechando o navegador do Twitter Trends.")
        driver.quit()

# Função para extrair tendências do Google Trends usando pytrends
def get_google_trends():
    logger.info("Iniciando extração de tendências do Google Trends...")

    pytrends = TrendReq(hl='pt-BR', tz=360)
    logger.info("API do pytrends configurada com sucesso.")

    try:
        logger.info("Obtendo tendências diárias do Google Trends...")
        trends = pytrends.trending_searches(pn='brazil')
        trends.columns = ['Hashtag']

        contagens = []

        for hashtag in trends['Hashtag']:
            logger.info(f"Consultando dados de interesse ao longo do tempo para: {hashtag}")
            pytrends.build_payload([hashtag], cat=0, timeframe='now 1-d', geo='BR')
            interest = pytrends.interest_over_time()

            if not interest.empty:
                last_value = interest[hashtag].iloc[-1]
                contagens.append(last_value)
            else:
                contagens.append(0)

        trends['Contagem'] = contagens

        tsv_filename = "google_trends.tsv"
        trends.to_csv(tsv_filename, sep='\t', index=False)
        logger.info(f"Tendências salvas em {tsv_filename}.")

        return trends

    except Exception as e:
        logger.error(f"Erro durante a extração das tendências do Google Trends: {e}")
        return pd.DataFrame(columns=["Hashtag", "Contagem"])

# Função principal para execução
def execute_get_hashtags():
    twitter_url = 'https://trends24.in/brazil/'

    twitter_trends_data = get_twitter_trends(twitter_url)
    google_trends_data = get_google_trends()

    if twitter_trends_data is not None:
        if google_trends_data is None or google_trends_data.empty:
            logger.warning("Dados do Google Trends estão ausentes ou vazios. Continuando com dados do Twitter apenas.")
            google_trends_data = pd.DataFrame(columns=["Hashtag", "Contagem"])

        combine_and_save_trends(twitter_trends_data, google_trends_data)
    else:
        logger.error("Erro ao obter dados do Twitter Trends. Verifique os logs para detalhes.")

    logger.info("Processo concluído.")

# Executar a função principal
if __name__ == "__main__":
    execute_get_hashtags()
