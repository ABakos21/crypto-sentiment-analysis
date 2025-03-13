
import os
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta

load_dotenv()
api_key = os.environ["NEWS_API_KEY"]
#print(api_key)


def fetch_news_api(keyword, date,api_key) :
    # NewsAPI endpoint
    url = (   'https://newsapi.org/v2/everything?'
       f'q={keyword}&'
       f'from={date}&'
       f'apikey={api_key}&'
       'sortBy=popularity&'
       'language=en'


          )
    #'to': date,    # End date (ISO format: YYYY-MM-DD)
     #f'apikey={api_key}&'
    # Send GET request to NewsAPI
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        print(response.status_code)
        articles = response.json()['articles']
        print(articles)
    else:
        print(f"Failed to fetch articles. Status code: {response.status_code}")

if __name__ == "__main__":
# Yesterday Data in YYYY-MM-DD format
 today_date = datetime.today()
 yesterday = today_date - timedelta(days=1)
 yesterday_format = yesterday.strftime('%Y-%m-%d')
 print(yesterday_format)
 fetch_news_api("Bitcoin", yesterday_format,api_key)
