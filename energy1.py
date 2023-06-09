###
# Her skal vi finde de senste produktions og eksport data, men ...
# for at kunne køre requestes igen senere (hvis det viser sig at have fejlet),
# skal vi bruge et tidsstempel til at definere hvad vi mener med "seneste"

# https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime?offset=0&start=2023-06-06T00:00&sort=Minutes5UTC%20DESC&timezone=dk

import datetime
import requests

now = datetime.datetime.now()

print(now.isoformat( timespec="minutes"))

def getLatestPowProdExp(timestamp: datetime.datetime):
    """ 
    Get the latest record at specific timestamp
    """        
    t = timestamp - datetime.timedelta(minutes=5)
    url = ' https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime'
    params = {
        'limit': 2,
        'offset': 0,
        'start': t.isoformat(timespec="minutes"),
        'timezone': 'dk6',
        'sort': 'Minutes5UTC ASC',
    }
    print(params)
    response = requests.get(url, params)

    if response.status_code == 200:
        json = response.json()
        if 'records' in json and len(json['records']) > 0:
            # print(json['records'])
            return json['records'][0:2]
        else:
            print(json)
    else:
        print(response.text)
    
print("Latest:", getLatestPowProdExp(datetime.datetime.now()))