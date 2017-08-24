import pandas as pd
import matplotlib.pyplot as plt
from dateutil import parser
import csv
import utilities as ut

pickle_in_file = 'cjczo.pkl'
df_CJCZO = pd.read_pickle(pickle_in_file)

pickle_in_file = 'lczo.pkl'
df_LCZO = pd.read_pickle(pickle_in_file)

pickle_in_file = 'ssczo.pkl'
df_SSCZO = pd.read_pickle(pickle_in_file)

pickle_in_file = 'sshczo.pkl'
df_SSHCZO = pd.read_pickle(pickle_in_file)

pickle_in_file = 'erczo.pkl'
df_ERCZO = pd.read_pickle(pickle_in_file)

pickle_in_file = 'bcczo.pkl'
df_BCCZO = pd.read_pickle(pickle_in_file)

desired_width = 200
pd.set_option('display.width', desired_width)
print('CJCZO')
df_CJCZO_descibed = df_CJCZO.describe()
print('LCZO')
df_LCZO_described = df_LCZO.describe()
print(df_LCZO.describe())
keys = ['SiteCode','DateTime','SampleCode']
dfCJCZO_LCZO_descibed = df_LCZO_described.merge(df_CJCZO_descibed, how='outer',suffixes=['_LCZO', '_CJCZO'])
print(dfCJCZO_LCZO_descibed)