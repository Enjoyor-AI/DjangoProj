import pickle
import json


fr = open('detector_plot.pkl', 'rb')
inf = pickle.load(fr)
fr.close()
print(inf)
print(type(inf))
n = json.loads(inf)
print(n)
print(type(n))
print(n['593'])