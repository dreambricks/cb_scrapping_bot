import os
import requests
import csv
import argparse
import logging
import config

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuração do token de autenticação
bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
if not bearer_token:
    logger.error("BEARER_TOKEN não encontrado. Configure-o como uma variável de ambiente.")

search_url = "https://api.twitter.com/2/tweets/search/recent"


def bearer_oauth(r):
    """
    Função de autenticação Bearer.
    """
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r


def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params)
    logger.info(f"Requisição para URL: {response.url}")
    if response.status_code != 200:
        logger.error(f"Erro na requisição: {response.status_code} - {response.text}")
        return None
    return response.json()


def fetch_mentions(username, max_results):
    max_results = max(10, min(max_results, 100))
    query = f"@{username} -is:retweet"
    query_params = {
        'query': query,
        'tweet.fields': 'created_at,author_id,text',
        'expansions': 'author_id',  # Expande para incluir dados do autor
        'user.fields': 'username',  # Inclui o campo username no retorno
        'max_results': max_results
    }
    logger.info(f"Buscando menções para @{username}...")
    response_json = connect_to_endpoint(search_url, query_params)

    # Mapeia os author_id para usernames
    users = {user["id"]: user["username"] for user in response_json.get("includes", {}).get("users", [])}
    tweets = response_json.get("data", []) if response_json else []

    # Substitui author_id pelo username no resultado
    for tweet in tweets:
        tweet['author_username'] = f"@{users.get(tweet['author_id'], 'desconhecido')}"
    return tweets


def fetch_tweets_by_hashtags(hashtags, max_results):
    max_results = max(10, min(max_results, 100))
    query = " OR ".join(f"#{hashtag}" for hashtag in hashtags) + " -is:retweet"
    query_params = {
        'query': query,
        'tweet.fields': 'created_at,author_id,text',
        'expansions': 'author_id',
        'user.fields': 'username',
        'max_results': max_results
    }
    logger.info(f"Buscando tweets com hashtags: {', '.join(hashtags)}")
    response_json = connect_to_endpoint(search_url, query_params)

    # Mapeia os author_id para usernames
    users = {user["id"]: user["username"] for user in response_json.get("includes", {}).get("users", [])}
    tweets = response_json.get("data", []) if response_json else []

    # Substitui author_id pelo username no resultado
    for tweet in tweets:
        tweet['author_username'] = f"@{users.get(tweet['author_id'], 'desconhecido')}"
    return tweets


def save_to_tsv(tweets, filename):
    if not tweets:
        logger.info(f"Nenhum dado para salvar em {filename}")
        return

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter='\t')
        writer.writerow(["Autor", "Texto"])  # Modificado para "Autor" (username)
        for tweet in tweets:
            cleaned_text = tweet['text'].replace('\n', ' ').replace('\t', ' ')
            writer.writerow(
                [tweet.get('author_username', 'desconhecido'), cleaned_text])  # Usa o username
    logger.info(f"Resultados salvos em {filename}")


def buscar_tweets_e_mencoes():
    parser = argparse.ArgumentParser(description="Buscar menções e tweets com hashtags.")
    parser.add_argument("--username", type=str, default="casasbahia", help="Usuário para buscar menções.")
    parser.add_argument("--hashtags", nargs="+", type=str, default=["blackfriday"],
                        help="Lista de hashtags para buscar tweets.")
    parser.add_argument("--max_results", type=int, default=40, help="Máximo de resultados (entre 10 e 100).")
    args = parser.parse_args()

    # Buscar menções ao usuário se o username for fornecido
    if args.username:
        mentions = fetch_mentions(args.username, args.max_results)
        output_folder = config.X_TSV_FOLDER_IN
        os.makedirs(output_folder, exist_ok=True)
        tsv_filename = os.path.join(output_folder, f"{args.username}_mentions.tsv")
        save_to_tsv(mentions, tsv_filename)

    # Buscar tweets com hashtags se hashtags forem fornecidas
    if args.hashtags:
        hashtag_tweets = fetch_tweets_by_hashtags(args.hashtags, args.max_results)
        tsv_filename = "hashtags_tweets.tsv"
        save_to_tsv(hashtag_tweets, tsv_filename)


if __name__ == "__main__":
    buscar_tweets_e_mencoes()
