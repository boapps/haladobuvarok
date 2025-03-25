import requests
import re
import json
from time import sleep


subsets = [d['id'] for d in requests.get('https://index.commoncrawl.org/collinfo.json').json()]
page = 'https://ingatlan.com/*'

paths = []
urls = set()

for sub in subsets:
    url = f"https://index.commoncrawl.org/{sub}-index?url={page}&output=json"
    resp = requests.get(url)
    if resp.status_code == 200:
        for raw_data in resp.text.splitlines():
            data = json.loads(raw_data)
            if re.match(r'https://ingatlan\.com/\d+', data['url']) and data['status'] == '200' and data['url'] not in urls:
                paths.append(data['url']+':'+data['filename']+':'+data['offset']+':'+data['length']+'\n')
                urls.add(data['url'])
    sleep(1)

with open('paths.txt', 'w') as f:
    f.writelines(paths)
