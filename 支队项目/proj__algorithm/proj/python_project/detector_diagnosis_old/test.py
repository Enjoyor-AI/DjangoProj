import requests
import json


url = 'http://192.168.25.22:8088/addMaintainList'
json_result = {"listName": "测试2清单", "listNode": [
    "04f23d97fdb1a5765896e5ec592cef48",
    "065b38ed979e9d2aad8b391bbb181669",
    "046fb043bfd6aa4264bb676dc0b9dec1"]}

s = json.dumps(json_result)
print(s)
r = requests.post(url, data=s)
print(r.text)