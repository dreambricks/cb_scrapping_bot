import logging
from apscheduler.schedulers.background import BackgroundScheduler
import gettwitterposts
import gettopproductsstate
import gethashtags
import getproductstrends
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='get_data_robot.log',
    filemode='a'
)
logger = logging.getLogger(__name__)

# Funções que executam as tarefas
def run_gettwitterposts():
    logger.info("Executando gettwitterposts...")
    try:
        gettwitterposts.main()
        logger.info("gettwitterposts executado com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao executar gettwitterposts: {e}")

def run_gettopproductsstate():
    logger.info("Executando gettopproductsstate...")
    try:
        gettopproductsstate.main()
        logger.info("gettopproductsstate executado com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao executar gettopproductsstate: {e}")

def run_gethashtags():
    logger.info("Executando gethashtags...")
    try:
        gethashtags.main()
        logger.info("gethashtags executado com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao executar gethashtags: {e}")

def run_getproductstrends():
    logger.info("Executando getproductstrends...")
    try:
        getproductstrends.main()
        logger.info("getproductstrends executado com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao executar getproductstrends: {e}")

if __name__ == "__main__":
    # Inicializa o agendador
    scheduler = BackgroundScheduler()

    # Agendar as tarefas
    scheduler.add_job(run_gettwitterposts, 'interval', minutes=10)
    scheduler.add_job(run_gettopproductsstate, 'interval', minutes=15)
    scheduler.add_job(run_gethashtags, 'interval', minutes=20)
    scheduler.add_job(run_getproductstrends, 'interval', minutes=30)

    # Iniciar o agendador
    scheduler.start()
    logger.info("Robô iniciado")

    # Manter o script em execução
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Encerrando o robô...")
        scheduler.shutdown()
