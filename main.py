import requests
import pandas as pd

headers = {"Authorization": "Bearer %########################################"}

url = "https://api.twitter.com/2/tweets/search/recent?query=MALARIA&max_results=100&expansions=author_id&tweet.fields=id,created_at,author_id&user.fields=description"
response = requests.request("GET", url, headers=headers).json()
print(response)

df = pd.DataFrame(response['data'])
df.to_csv('response_python.csv')

