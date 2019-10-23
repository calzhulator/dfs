from googleapiclient.discovery import build

my_api_key = "AIzaSyDx8_U8dI9SyEMpWq2PM1Q6H1tQpB_W0c0"
my_cse_id = "011876155113649707850:rwpmxdk0yms"


def first_link_result(text):
    service = build("customsearch", "v1", developerKey=my_api_key)
    res = service.cse().list(q=text, cx=my_cse_id).execute()
    return res['items'][0]['link']
