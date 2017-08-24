import pandas
import matplotlib.pyplot as plt
from dateutil import parser
import csv
# ERCZOElderCreekEditHeaders.csv

def get_type(v):
    try:
        x = float(v)
        return 'float'
    except ValueError:
        pass
    return 'str'


f = open('ERCZOElderCreekEditHeaders.csv','r')
reader = csv.reader(f)
i = 0
site_code_to_name = {'Elder Creek': 'Elder_Creek_', }

with open('ERCZOElderCreek_site_codes_updated.csv', 'w',newline='') as file:
    writer = csv.writer(file,delimiter=',')
    deletecols = []
    for row in reader:
        i += 1
        if i > 2:
            # print(row[1])
            if row[3] in site_code_to_name:
                site_name = site_code_to_name[row[3]]
                row[3] = "ERCZO_" + site_name
                k=0
                #print('delete values')
                #print(len(row))
                for deletion in deletecols:
                    if deletion ==1:
                        del row[k]
                        k-=1
                        print(k)
                    k+=1
                writer.writerow(row)
        else:
            cellcount = len(row)
            k=0
            #print('starting deletion')
            #try:
            for k in range(0,cellcount):
                print(row[k])
                if row[k].endswith('_SD') or '(HR)' in row[k]:
                    #print(row[k])
                    #print(k)
                    deletecols.append(1)
                    #del row[k]
                    #cellcount-=1
                else:
                    deletecols.append(0)
            k = 0
            for deletion in deletecols:
                if deletion == 1:
                    del row[k]
                    k-=1
                k += 1
            #except IndexError:
            #print(k)
            writer.writerow(row)

def extract_df_info_ERCZO(csv_file):
    header = []
    units = []
    method = []

    # get the header and datatypes
    with open(csv_file, 'rb') as f:
        lines = f.readlines()
        header = lines[0].strip().decode().split(',')
        unit_labels = lines[1].strip().decode("windows-1252").split(',')
        # method = lines[2].strip().decode().split(',')
        data = lines[2].strip().decode().split(',')
    types = [get_type(v) for v in data]

    # reformat the column names
    colnames = []
    print('header')
    for i in range(len(header)):
        print(header[i])
        header[i] = header[i].split('(')[0]
        u = unit_labels[i] if unit_labels[i] != '' else 'na'
        unit_labels[i] = u
        colnames.append('%s (%s)' % (header[i], u))
        # print('%s (%s) type: %s' % (header[i], u, types[i]))

    return colnames, unit_labels, types, method

f = 'ERCZOElderCreek_site_codes_updated.csv'
print('Processing: %s' % f)
indexcols =[0,3]
# read these data into a pandas dataframe.  Use "DateTime" as the index column
cols, units, types, methods = extract_df_info_ERCZO(f)
parse_dates = [cols[4]]
dtypes = dict(zip(cols, types))

# read each file into a temporary dataframe
df_ERCZO = pandas.read_csv(f, sep=',', names=cols, dtype=dtypes,
                          parse_dates=parse_dates, na_values=['-9999', 'NA', 'BAD READING', 'ND'],
                          skiprows=2) #, index_col=indexcols
print(df_ERCZO.describe())


def extract_df_info_ERCZOMethod(csv_file):
    header = []
    units = []
    method = []

    # get the header and datatypes
    with open(csv_file, 'rb') as f:
        lines = f.readlines()
        header = lines[0].strip().decode().split(',')
        unit_labels = lines[1].strip().decode("windows-1252").split(',')
        # method = lines[2].strip().decode().split(',')
        data = lines[2].strip().decode().split(',')
    types = [get_type(v) for v in data]

    # reformat the column names
    colnames = []
    for i in range(len(header)):
        header[i] = header[i].split('(')[0]
        u = unit_labels[i] if unit_labels[i] != '' else 'na'
        unit_labels[i] = u
        colnames.append('%s %s' % (header[i], u))
        # print('%s (%s) type: %s' % (header[i], u, types[i]))

    return colnames, unit_labels, types, method


f = 'EelMethods45filteronly.csv'
print('Processing: %s' % f)
indexcols =[1,2]
# read these data into a pandas dataframe.  Use "DateTime" as the index column
cols, units, types, methods = extract_df_info_ERCZO(f)
parse_dates = [cols[4]]
dtypes = dict(zip(cols, types))

# read each file into a temporary dataframe
df_ERCZOMethod = pandas.read_csv(f, sep=',', names=cols, dtype=dtypes,
                          parse_dates=parse_dates, na_values=['-9999', 'NA', 'BAD READING', 'ND'],
                          skiprows=2) #, index_col=indexcols
print(df_ERCZOMethod.describe())

print('Number of rows in ER Results df: %d' % len(df_ERCZO))
print('Number of rows in ER methods df: %d' % len(df_ERCZOMethod))
print(df_ERCZO.columns)
print(df_ERCZOMethod.columns)
dfs=[df_ERCZO,df_ERCZOMethod]
#df = pandas.concat(dfs, join='inner', keys=['SampleCode', 'DateTime']) # outer
df = df_ERCZO.merge(df_ERCZOMethod, how='left', on=['SampleCode (na)','SiteCode (na)'])

print(df.columns)
del df['DateTime (na)_x']
df['DateTime'] = df['DateTime (na)_y']
del df['DateTime (na)_y']
csv_out_file = 'ERMethodsAndResultsMerged.csv'
df.to_csv(csv_out_file)
print('Number of rows in combo df: %d' % len(df))
print(df.describe())

