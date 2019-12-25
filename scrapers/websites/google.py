from googleapiclient.discovery import build
from functools import lru_cache

my_api_key = "AIzaSyDx8_U8dI9SyEMpWq2PM1Q6H1tQpB_W0c0"
my_cse_id = "011876155113649707850:rwpmxdk0yms"
overrides = {'profootballreference team ten': ['https://www.pro-football-reference.com/teams/oti/index.htm']}


@lru_cache(maxsize=None)
def link_results(text):
    if text in overrides.keys():
        print("Using google override for: " + text)
        return overrides[text]
    else:
        print("Googling results for: " + text)
        service = build("customsearch", "v1", developerKey=my_api_key)
        res = service.cse().list(q=text, cx=my_cse_id).execute()
        return [it['link'] for it in res['items']]
