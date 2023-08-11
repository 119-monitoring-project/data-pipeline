import json
import requests
import xmltodict

url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEgytListInfoInqire'
params = {'serviceKey' : 'i2nQ5zrOUo6vBQs0nD+h14vmsQMPQuEvfvf+P5BfVQSgkWFMKabetmAYmLLk79BRx7eizGH3707qbtns1K7Z2g==', 'pageNo' : '1', 'numOfRows' : '9999' }


# API 호출
def call_api(url, params):
    response = requests.get(url, params=params)
    xmlString = response.text
    jsonString = xmltodict.parse(xmlString)
    data = jsonString['response']['body']['items']['item']

    return data


print(call_api(url=url, params=params))