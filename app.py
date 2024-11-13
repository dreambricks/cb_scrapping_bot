import logging
from apscheduler.schedulers.background import BackgroundScheduler
import gettwitterposts
import gettopproductsstate
import gethashtags
import getproductstrends
import time
import os

def create_logger(task_name):
    logger = logging.getLogger(task_name)
    folder_logs = 'botlogs'
    os.makedirs(folder_logs, exist_ok=True)
    handler = logging.FileHandler(f'{folder_logs}/{task_name}.log', mode='a')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

# Loggers individuais
logger_twitterposts = create_logger('gettwitterposts')
logger_topproductsstate = create_logger('gettopproductsstate')
logger_hashtags = create_logger('gethashtags')
logger_productstrends = create_logger('getproductstrends')

# Funções que executam as tarefas
def run_gettwitterposts():
    logger_twitterposts.info("Executando gettwitterposts...")
    try:
        gettwitterposts.buscar_tweets_e_mencoes()
        logger_twitterposts.info("gettwitterposts executado com sucesso.")
    except Exception as e:
        logger_twitterposts.error(f"Erro ao executar gettwitterposts: {e}")

def run_gettopproductsstate():
    logger_topproductsstate.info("Executando gettopproductsstate...")
    try:
        gettopproductsstate.execute_getproductsstate()
        logger_topproductsstate.info("gettopproductsstate executado com sucesso.")
    except Exception as e:
        logger_topproductsstate.error(f"Erro ao executar gettopproductsstate: {e}")

def run_gethashtags():
    logger_hashtags.info("Executando gethashtags...")
    try:
        gethashtags.execute_get_hashtags()
        logger_hashtags.info("gethashtags executado com sucesso.")
    except Exception as e:
        logger_hashtags.error(f"Erro ao executar gethashtags: {e}")

def run_getproductstrends():
    logger_productstrends.info("Executando getproductstrends...")
    try:
        getproductstrends.execute_getproducts_trends()
        logger_productstrends.info("getproductstrends executado com sucesso.")
    except Exception as e:
        logger_productstrends.error(f"Erro ao executar getproductstrends: {e}")

if __name__ == "__main__":
    # Executar as tarefas uma vez no início
    logger_twitterposts.info("Executando gettwitterposts pela primeira vez ao iniciar.")
    run_gettwitterposts()

    logger_topproductsstate.info("Executando gettopproductsstate pela primeira vez ao iniciar.")
    run_gettopproductsstate()

    logger_hashtags.info("Executando gethashtags pela primeira vez ao iniciar.")
    run_gethashtags()

    logger_productstrends.info("Executando getproductstrends pela primeira vez ao iniciar.")
    run_getproductstrends()

    # Inicializa o agendador
    scheduler = BackgroundScheduler()

    # Agendar as tarefas
    scheduler.add_job(run_gettwitterposts, 'interval', minutes=15)
    scheduler.add_job(run_gettopproductsstate, 'interval', minutes=60)
    scheduler.add_job(run_gethashtags, 'interval', minutes=60)
    scheduler.add_job(run_getproductstrends, 'interval', minutes=30)

    # Iniciar o agendador
    scheduler.start()

    logger_twitterposts.info("Robô iniciado - gettwitterposts")
    logger_topproductsstate.info("Robô iniciado - gettopproductsstate")
    logger_hashtags.info("Robô iniciado - gethashtags")
    logger_productstrends.info("Robô iniciado - getproductstrends")

    # Manter o script em execução
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger_twitterposts.info("Encerrando o robô - gettwitterposts...")
        logger_topproductsstate.info("Encerrando o robô - gettopproductsstate...")
        logger_hashtags.info("Encerrando o robô - gethashtags...")
        logger_productstrends.info("Encerrando o robô - getproductstrends...")
        scheduler.shutdown()
