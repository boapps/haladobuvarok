import gzip
import io
import requests
import json
from time import sleep

with open('paths.txt') as f:
    paths = f.readlines()

done_urls = set()

# with open('data.jsonl') as file:
#     for line in file:
#         data = json.loads(line)
#         done_urls.add(data['url'])

with open('data.jsonl', 'a') as file:
    for p in paths:
        data = p.split(':')
        print(data)
        ourl = ':'.join([data[0], data[1]])
        if ourl in done_urls:
            continue
        segment_file = data[2]
        offset = int(data[3])
        length = int(data[4])
        url = f'https://data.commoncrawl.org/{segment_file}'
        response = requests.get(url, headers={'Range': f'bytes={offset}-{offset+length-1}'})
        if response.status_code == 206:
            content = response.content
            # Decompress the data
            with gzip.open(io.BytesIO(content), 'rb') as f:
                content = f.read()
            print(content)
            file.write(json.dumps({'url': ourl, 'content': content.decode('utf-8')}, ensure_ascii=False)+'\n')
        else:
            print(f"Failed to fetch data: {response.status_code}")
        sleep(1)
