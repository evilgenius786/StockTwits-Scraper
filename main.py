import threading

import json
import os.path
import time
from datetime import datetime

import requests

if os.path.isfile('proxy.txt'):
    with open('proxy.txt') as f:
        p = f.read()
        print(f"Using proxy: {p}")
        proxy = {"http": p, "https": p}
else:
    proxy = None
start_id = 493446702
end_id = 380000000
num_threads = 3

created_at_limit = datetime.strptime("2021-09-30", '%Y-%m-%d')
stocktwits = "https://api.stocktwits.com/api/2/streams/symbol/SPY.json"


def process(start, end):
    max_ = start
    print(f"Start: {start}, End: {end} URL: ({stocktwits}?max={start})")
    while max_ > end:
        file = f"./json/{start}-{max_}.json"
        # if os.path.isfile(file):
        #     print(f"[+] {file} already exists, skipping")
        #     max_ += 30
        #     continue
        data = getJson(f"{stocktwits}?max={max_}")
        current_created_at = datetime.strptime(data['messages'][-1]['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        if current_created_at < created_at_limit:
            print(f"Current created_at ({current_created_at}) is less than limit ({created_at_limit})")
            break
        time.sleep(1)
        with open(file, 'w') as f:
            json.dump(data, f, indent=4)
        max_ = data['messages'][-2]['id']
        since = data['cursor']['since']
        print(f"Start: {start} Since: {since} Max: {max_}")


def combineMessages():
    print("[+] Combining messages")
    messages = []
    for file in os.listdir("json"):
        with open(f"json/{file}") as f:
            data = json.load(f)
            for message in data['messages']:
                if message not in messages:
                    messages.append(message)
    with open("messages.json", 'w') as f:
        json.dump(messages, f, indent=4)
    print("[+] Done")


def main():
    logo()
    if not os.path.isdir("json"):
        os.mkdir("json")
    # combineMessages()
    diff = end_id - start_id
    step = diff // num_threads
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=process, args=(start_id + step * i, start_id + step * (i + 1),))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    # combineMessages()


def getJson(url):
    return requests.get(url, proxies=proxy).json()


def logo():
    print(r"""
  _________ __                 __   ___________       .__  __          
 /   _____//  |_  ____   ____ |  | _\__    ___/_  _  _|__|/  |_  ______
 \_____  \\   __\/  _ \_/ ___\|  |/ / |    |  \ \/ \/ /  \   __\/  ___/
 /        \|  | (  <_> )  \___|    <  |    |   \     /|  ||  |  \___ \ 
/_______  /|__|  \____/ \___  >__|_ \ |____|    \/\_/ |__||__| /____  >
        \/                  \/     \/                               \/ 
=======================================================================
                StockTwits scraper by @evilgenius786
=======================================================================
[+] API Based
[+] JSON Output
_______________________________________________________________________
""")


if __name__ == '__main__':
    main()
