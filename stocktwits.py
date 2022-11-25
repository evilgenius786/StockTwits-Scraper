import threading
import traceback
from random import randint

import json
import os.path
import time
from datetime import datetime

import requests

# if os.path.isfile('proxy.txt'):
#     with open('proxy.txt') as f:
#         p = f.read()
#         print(f"Using proxy: {p}")
#         proxy = {"http": p, "https": p}
# else:
proxy = None
end_id = 380000000
num_threads = 50

created_at_limit = datetime.strptime("2021-09-30", '%Y-%m-%d')
stocktwits = "https://api.stocktwits.com/api/2/streams/symbol"


# ticker_max = {}


def process(ticker, start, end):
    # global ticker_max
    max_ = start
    print(f"Start: {start}, End: {end} URL: ({stocktwits}/{ticker}.json?max={start})")
    if os.path.isfile(f"{ticker}/{start}.txt"):
        with open(f"{ticker}/{start}.txt") as f:
            max_ = int(f.read())
        print(f"Jumping from {start} to {max_}")
    found = False
    while max_ > end and not found:
        while True and not found:
            try:
                file = f"./{ticker}/{start}-{max_}.json"
                # if f"{max_}" in ticker_max.keys():
                #     print(f"Taking {max_} to {ticker_max[str(max_)]} from cache")
                #     max_ = ticker_max[str(max_)]
                #     continue
                data = getJson(f"{stocktwits}/{ticker}.json?max={max_}")
                current_created_at = datetime.strptime(data['messages'][-1]['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                if current_created_at < created_at_limit:
                    print(f"Current created_at ({current_created_at}) is less than limit ({created_at_limit})")
                    found = True
                    break
                time.sleep(1)
                with open(file, 'w') as f:
                    json.dump(data, f, indent=4)
                tmp = data['messages'][-2]['id']
                # ticker_max[str(max_)] = tmp
                max_ = tmp
                since = data['cursor']['since']
                print(f"Start: {start} Since: {since} Max: {max_}")
                with open(f"{ticker}/{start}.txt", 'w') as f:
                    f.write(str(max_))
                # with open(f"{ticker}/max.json", 'w') as f:
                #     json.dump(ticker_max, f, indent=4)
                break
            except:
                traceback.print_exc()


def main():
    # global ticker_max
    logo()
    tickers = []
    if os.path.isfile("tickers.txt"):
        with open("tickers.txt") as f:
            tickers = f.read().splitlines()
    else:
        print("[-] No tickers.txt file found")
        exit(1)
    for ticker in tickers:
        print(f"[+] Scraping ticker {ticker}")
        if not os.path.isdir(ticker):
            os.mkdir(ticker)
        start_id = getJson(f"{stocktwits}/{ticker}.json")['cursor']['since']
        # if os.path.isfile(f"{ticker}/max.json"):
        #     print(f"[+] {ticker}/max.json already exists, loading from cache...")
        #     with open(f"{ticker}/max.json") as f:
        #         ticker_max = json.load(f)
        #     for key in ticker_max:
        #         start_id = int(key)
        #         break
        if os.path.isfile(f"{ticker}/start.txt"):
            with open(f"{ticker}/start.txt") as f:
                start_id = int(f.read())
        else:
            with open(f"{ticker}/start.txt", 'w') as f:
                f.write(str(start_id))
        diff = end_id - start_id
        step = diff // num_threads
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=process, args=(ticker, start_id + step * i, start_id + step * (i + 1),))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()


def getJson(url):
    time.sleep(randint(1, 5))
    while True:
        try:
            r = requests.get(url, proxies=proxy)
            if r.status_code == 200:
                return r.json()
            else:
                print(f"[-] Error: {r.status_code}")
                time.sleep(1)
                return getJson(url)
        except Exception as e:
            print(f"[-] Error: {e}")
            time.sleep(1)
            return getJson(url)
    # return requests.get(url, proxies=proxy).json()


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
