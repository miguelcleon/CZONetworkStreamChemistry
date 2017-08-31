import pandas as pd
import matplotlib.pyplot as plt
from dateutil import parser
import csv
import utilities as ut

desired_width = 200
pd.set_option('display.width', desired_width)

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


df_BCCZO_described = df_BCCZO.describe()
df_LCZO_described = df_LCZO.describe()
df_CJCZO_descibed = df_CJCZO.describe()

df_SSCZO_described = df_SSCZO.describe()
df_SSHCZO_described = df_SSHCZO.describe()
df_ERCZO_described = df_ERCZO.describe()


keys = ['SiteCode','DateTime','SampleCode']
newcolumns = []
for column in df_BCCZO_described.columns:
    newcolumns.append(column + "_BCCZO")
df_BCCZO_described.columns = newcolumns

newcolumns = []
for column in df_LCZO_described.columns:
    newcolumns.append(column + "_LCZO")
df_LCZO_described.columns = newcolumns

newcolumns = []
for column in df_CJCZO_descibed.columns:
    newcolumns.append(column + "_CJCZO")
df_CJCZO_descibed.columns = newcolumns

newcolumns = []
for column in df_SSHCZO_described.columns:
    newcolumns.append(column + "_SSHCZO")
df_SSHCZO_described.columns = newcolumns

newcolumns = []
for column in df_ERCZO_described.columns:
    newcolumns.append(column + "_ERCZO")
df_ERCZO_described.columns = newcolumns
print(df_ERCZO_described)
newcolumns = []
for column in df_SSCZO_described.columns:
    newcolumns.append(column + "_SSCZO")
df_SSCZO_described.columns = newcolumns

dfBCCZO_LCZO_descibed= pd.merge(df_BCCZO_described, df_LCZO_described, left_index=True, right_index=True)

dfCJCZO__BCLCZO_descibed= pd.merge(dfBCCZO_LCZO_descibed, df_CJCZO_descibed, left_index=True, right_index=True)

dfERCZO__CJBCLCZO_descibed= pd.merge(dfCJCZO__BCLCZO_descibed, df_ERCZO_described, left_index=True, right_index=True)

dfSSCZO__ERCJBCLCZO_descibed= pd.merge(dfERCZO__CJBCLCZO_descibed, df_SSCZO_described, left_index=True, right_index=True)

dfSSHCZO__SSERCJBCLCZO_descibed= pd.merge(dfSSCZO__ERCJBCLCZO_descibed, df_SSHCZO_described, left_index=True, right_index=True)


dfSSHCZO__SSERCJBCLCZO_descibed=dfSSHCZO__SSERCJBCLCZO_descibed.reindex_axis(sorted(dfSSHCZO__SSERCJBCLCZO_descibed.columns), axis=1)


csv_out_file = 'dfXCZO_descibed.csv'
dfSSHCZO__SSERCJBCLCZO_descibed.to_csv(csv_out_file)
#
# csv_out_file = 'dfCJCZO_LCZO_descibed.csv'
# dfCJCZO_LCZO_descibed.to_csv(csv_out_file)
# #print("ERCZO")
# #print(df_ERCZO.describe())
# df_ERCZO_describe = df_ERCZO.describe()
# csv_out_file = 'ERCZO_descibed.csv'
# df_ERCZO_describe.to_csv(csv_out_file)
#
# dfERCZO_CJCZO_descibed= pd.merge(df_CJCZO_descibed, df_ERCZO_describe, left_index=True, right_index=True, suffixes=('_CJCZO', '_ERCZO'))
# csv_out_file = 'dfCJCZO_ERCZO_descibed.csv'
# dfERCZO_CJCZO_descibed.to_csv(csv_out_file)

