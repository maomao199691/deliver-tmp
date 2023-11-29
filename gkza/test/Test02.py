import requests
import time

# https://www.23hh.com/book/32792/32792728/
#url = "https://www.23hh.com/book/32792/32792728/83720731.html"


url = "https://www.baidu.com"

max_retries = 3

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
}

for attempt in range(max_retries):
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            print(data)
            break
        else:
            print("eror")
    except requests.exceptions.RequestException as e:
        print(" ==> ", e)

    time.sleep(1)
else:
    print("Max try")