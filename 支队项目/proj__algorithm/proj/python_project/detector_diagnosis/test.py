import requests
import json
import pickle

# url = 'http://192.168.25.22:8088/addMaintainList'
# json_result = {"listName": "测试4清单", "listNode": [
#     "04f23d97fdb1a5765896e5ec592cef48",
#     "065b38ed979e9d2aad8b391bbb181669",
#     "046fb043bfd6aa4264bb676dc0b9dec1"]}
#
# s = json.dumps(json_result)
# print(s)
# r = requests.post(url, data=s)
# print(r.text)

a = {}
a[1] = 2
m = json.dumps(a)
print(type(m))
with open('test.pkl', 'wb') as f:
    pickle.dump(m, f, pickle.HIGHEST_PROTOCOL)
f.close()

fr = open('test.pkl', 'rb')
inf = pickle.load(fr)
fr.close()
print(inf)
print(type(inf))
n = json.loads(inf)
print(n)
print(type(n))