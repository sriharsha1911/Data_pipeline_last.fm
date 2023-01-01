
import requests
import json
from IPython.core.display import clear_output
import time 
import pandas as pd
import io

result=[]
def main():   
    pages=1
    total_pages=101
    
    while pages<total_pages:
        parameters={
        'method': 'chart.gettopartists',
        'page': pages
        }
        print("Requesting page {}/{}".format(pages, total_pages-1))
        clear_output(wait = True)
        time.sleep(3)
        get_res = last_fm_get(parameters)
        if(get_res.status_code!=200):
            print(get_res.text)
            break
        result.append(get_res.json())
        pages=pages+1
    #print(json.dumps(result[0],sort_keys=True,indent=4))
    frames = [pd.DataFrame(r['artists']['artist']) for r in result]
    artists = pd.concat(frames)
    print(artists)
    #artists.to_csv (r'C:\Users\svajjal\artists.csv', index = None, header=True) 

    from azure.storage.blob import (
        BlockBlobService
    )

  


    output = artists.to_csv (index_label="idx", encoding = "utf-8")
    #print(output)

    accountName = "etlprojectstorag"
    accountKey = "qy8VysCZ0j7UIxGOP5CKKqJQY/+9hEuGdBypXuCpg8XPTxSlaHTcHn8HIt2tUS4Xl+Lv5yB+far3+AStqFQbrQ=="
    containerName = "artistsdata"
    #blobName = "test3.json"

    blobService = BlockBlobService(account_name=accountName, account_key=accountKey)

    blobService.create_blob_from_text('artistsdata', 'artists.csv', output)


def last_fm_get(parameters):
    
    header={
        'user-agent': 'harsha1010'
    }
    url='https://ws.audioscrobbler.com/2.0/'
    parameters['api_key']='8da8b9765ef47b1629f0dcb9d28341a7'
    parameters['format']='json'
    parameters['limit']=50
    r=requests.get(url,headers=header,params=parameters)
    #print(r.status_code)
    return r


if __name__ == "__main__":
    main()


