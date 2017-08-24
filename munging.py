import pandas
import pandas as pd
import matplotlib.pyplot as plt
from dateutil import parser
import csv
import utilities as ut
# preprocess files


pickle_in_file = 'cjczo-jemez-agg.pkl'
#df.to_csv(csv_out_file)

df = pd.read_pickle(pickle_in_file)


f = open('LCZOStreamChemEditedHeaders.csv','r')
reader = csv.reader(f)
i=0
site_code_to_name = {'QS': 'Quebrada_Sonadora_', 'QT': 'Quebrada_Tronja_', 'RI':'Rio_Icacos_','Q2':'Bisley_Quebrada_2_',
                     'Q3': 'Bisley_Quebrada_3_','Q1':'Bisley_Quebrada_1_','MPR':'Mameyes_Puente_Roto_',
                     'QPA':'Quebrada_Pierta_A_','QPB':'Quebrada_Pierta_B_','QP':'Quebrada_Pierta_',
                     'QG':'Quebrada_Guaba_', 'RS':'Rio_Sabana_','RES4':'Rio_Espirtu_Santo_4_'}

with open('LCZOStreamChem_site_codes_updated.csv', 'w',newline='') as file:
    writer = csv.writer(file,delimiter=',')
    for row in reader:
        i += 1
        if i > 1:
            # print(row[1])
            if row[0] in site_code_to_name:
                site_name = site_code_to_name[row[0]]
                row[0] = "LCZO_" + site_name + row[0]
                writer.writerow(row)
        else:
            writer.writerow(row)



def extract_df_info_Luquillo(csv_file):
    header = []
    units = []
    method = []

    # get the header and datatypes
    with open(csv_file, 'rb') as f:
        lines = f.readlines()
        header = lines[0].strip().decode().split(',')
        data = lines[1].strip().decode().split(',')
    types = [ut.get_type(v) for v in data]

    # reformat the column names
    colnames = []
    unit_labels = []
    for i in range(len(header)):
        unit_label = header[i][header[i].find("("):header[i].find(")")+1]
        unit_labels.append(unit_label)
        print(header[i])
        print(unit_label)
        u = unit_label if unit_label != '' else 'na'
        unit_label = u
        colnames.append(header[i])

    return colnames, unit_labels, types, method

f.close()
f = 'LCZOStreamChem_site_codes_updated.csv'
print('Processing: %s' % f)
indexcols =[0,1,3]
# read these data into a pandas dataframe.  Use "DateTime" as the index column
cols, units, types, methods = extract_df_info_Luquillo(f)
parse_dates = [cols[3]]
dtypes = dict(zip(cols, types))

# read each file into a temporary dataframe
df_Luquillo = pandas.read_csv(f, sep=',', names=cols, dtype=dtypes,
                          parse_dates=parse_dates, na_values=['-9999', 'missing', 'BAD READING'],
                          skiprows=1, index_col=indexcols)
print('Number of rows in temp DF: %d' % len(df_Luquillo))

# make changes to Luquillo CZO data

NO3_mgl = df_Luquillo['NO3-N (ug N/L)'] / 1000
df_Luquillo['NO3 (mg/L)'] = NO3_mgl
del df_Luquillo['NO3-N (ug N/L)']

matched = []
for i in range(0, len(df.columns)):
    try:
        idx = df_Luquillo.columns.get_loc(df_Luquillo.filter(like=df.columns[i].split('(')[0]).columns[0])
        print ( '{:30s} {:30s}'.format(df.columns[i], df_Luquillo.columns[idx]))
        matched.append(df_Luquillo.columns[idx])
    except Exception:
        print ('{:30s} {:30s}'.format(df.columns[i], '---' ))
for i in range(0, len(df_Luquillo.columns)):
    if df_Luquillo.columns[i] not in matched:
        print ('{:30s} {:30s}'.format('---', df_Luquillo.columns[i] ))
print('CJCZO')
print(df.describe())



# convert NO3 from ug to mg


print('LCZO')
# print(df_Luquillo.describe())
# print(df_Luquillo.head())
dfs = [df, df_Luquillo]
# df =df.join(df_Luquillo, how='outer') #on=indexes,
# print('Number of rows in CJCZO df: %d' % len(df))
# print('Number of rows in Luquillo df: %d' % len(df_Luquillo))
df = pandas.concat(dfs, join='outer') # join='outer'
# print(df.describe())
# print('Number of rows in combined df: %d' % len(df))


pickle_out_file = 'lczo.pkl'
df_Luquillo.to_pickle(pickle_out_file)
csv_out_file = 'cjczo-lczo-agg.csv'
df.to_csv(csv_out_file)

# SSCZOStreamChemEditedHeaders.csv

f = open('SSCZOStreamChemEditedHeaders.csv','r')
reader = csv.reader(f)
i = 0
site_code_to_name = {'P300': 'Providence_Creek_headwaters_', 'P301': 'Providence_Creek_Subcatchment_',
                     'P303':'Providence_Creek_Subcatchment_','P304':'Providence_Creek_Subcatchment_',
                     'D102': 'Duff_Creek_','B200':'Bull_Creek_','B201':'Bull_Creek_subcatchment_',
                     'B203':'Bull_Creek_subcatchment_','B204':'Bull_Creek_subcatchment_','T003':'Teakettle_creek_',}

with open('SSCZOStreamChem_site_codes_updated.csv', 'w',newline='') as file:
    writer = csv.writer(file,delimiter=',')
    for row in reader:
        i += 1
        if i > 3:
            # print(row[1])
            if row[2] in site_code_to_name:
                site_name = site_code_to_name[row[2]]
                row[2] = "SSCZO_" + site_name + row[2]
                writer.writerow(row)
        else:
            writer.writerow(row)

def extract_df_info_SSCZO(csv_file):
    header = []
    units = []
    method = []

    # get the header and datatypes
    with open(csv_file, 'rb') as f:
        lines = f.readlines()
        header = lines[0].strip().decode().split(',')
        unit_labels = lines[1].strip().decode("windows-1252").split(',')
        method = lines[2].strip().decode().split(',')
        data = lines[3].strip().decode().split(',')
    types = [ut.get_type(v) for v in data]

    # reformat the column names
    colnames = []
    for i in range(len(header)):
        u = unit_labels[i] if unit_labels[i] != '' else 'na'
        unit_labels[i] = u
        colnames.append('%s %s' % (header[i], u))
        # print('%s (%s) type: %s' % (header[i], u, types[i]))

    return colnames, unit_labels, types, method

f = 'SSCZOStreamChem_site_codes_updated.csv'
print('Processing: %s' % f)
indexcols =[2,1,3]
# read these data into a pandas dataframe.  Use "DateTime" as the index column
cols, units, types, methods = extract_df_info_SSCZO(f)
parse_dates = [cols[3]]
dtypes = dict(zip(cols, types))

# read each file into a temporary dataframe
df_SSCZO = pandas.read_csv(f, sep=',', names=cols, dtype=dtypes,
                          parse_dates=parse_dates, na_values=['-9999', 'NA', 'BAD READING', 'ND'],
                          skiprows=2, index_col=indexcols)


Alkalinity_mol_weight = 61.0168
Alkalinity_mgl = df_SSCZO['Alkalinity (ueq L-1)'] * Alkalinity_mol_weight / 1000
df_SSCZO['Alkalinity (mg/L)'] = Alkalinity_mgl
H_mol_weight = 1.00794
H_mgl = df_SSCZO['H+ (ueq L-1)'] * H_mol_weight / 1000
df_SSCZO['H+ (mg/L)'] = H_mgl

EC_mS = df_SSCZO['EC - Electrical Conductivity  (mS/cm)'] * 1000
df_SSCZO['EC (uS/cm)'] = EC_mS
del df_SSCZO['Alkalinity (ueq L-1)']
del df_SSCZO['H+ (ueq L-1)']
del df_SSCZO['EC - Electrical Conductivity  (mS/cm)']
print(df_SSCZO.describe())

dfs = [df, df_SSCZO]
# df =df.join(df_Luquillo, how='outer') #on=indexes,
print('Number of rows in l and cjczo df: %d' % len(df))
print('Number of rows in SSCZO df: %d' % len(df_SSCZO))
df = pandas.concat(dfs, join='outer') # outer


pickle_out_file = 'ssczo.pkl'
df_SSCZO.to_pickle(pickle_out_file)
csv_out_file = 'cjczo-lczo-ssczo-agg.csv'
df.to_csv(csv_out_file)
print(df.describe())
print('Number of rows in combo df: %d' % len(df))

# ERCZOElderCreekEditHeaders.csv

f = open('SSHCZO - ShaversCreek2014EditHeaders.csv','r')
reader = csv.reader(f)
i = 0
site_code_to_name = {'SCAL': 'Shavers_Creek_above_Lake_Perez_','SCBL': 'Shavers_Creek_below_Lake_Perez_','SCO': 'Shavers_Creek_Outlet_',
                     'SH_WEIR': 'Shale_Hills_stream_outlet_by_weir_', }

with open('SSHCZOStreamChem_ShaversCreek2014_site_codes_updated.csv', 'w',newline='') as file:
    writer = csv.writer(file,delimiter=',')
    for row in reader:
        i += 1
        if i > 5:
            # print(row[1])
            if row[1] in site_code_to_name:
                site_name = site_code_to_name[row[1]]
                row[1] = "SSHCZO_" + site_name
                writer.writerow(row)
        else:
            writer.writerow(row)


def extract_df_info_SSHCZO(csv_file):
    header = []
    units = []
    method = []

    # get the header and datatypes
    with open(csv_file, 'rb') as f:
        lines = f.readlines()
        header = lines[0].strip().decode().split(',')
        unit_labels = lines[1].strip().decode("windows-1252").split(',')
        method = lines[2].strip().decode().split(',')
        data = lines[5].strip().decode("windows-1252").split(',')

    types = [ut.get_type(v) for v in data]

    # reformat the column names
    colnames = []
    for i in range(len(header)):
        u = unit_labels[i] if unit_labels[i] != '' else 'na'
        unit_labels[i] = u
        colnames.append('%s (%s)' % (header[i], u))
        # print('%s (%s) type: %s' % (header[i], u, types[i]))

    return colnames, unit_labels, types, method

f = 'SSHCZOStreamChem_ShaversCreek2014_site_codes_updated.csv'
print('Processing: %s' % f)
indexcols =[1,2,0]
# read these data into a pandas dataframe.  Use "DateTime" as the index column
cols, units, types, methods = extract_df_info_SSHCZO(f)
parse_dates = [cols[0]]
dtypes = dict(zip(cols, types))

# read each file into a temporary dataframe
df_SSHCZO = pandas.read_csv(f, sep=',', names=cols, dtype=dtypes,
                          parse_dates=parse_dates, na_values=['< 0.03', 'n.a.', '< 0.30', '< 0.3', 'ND'],
                          skiprows=5, index_col=indexcols)



# df =df.join(df_Luquillo, how='outer') #on=indexes,
print('Number of rows in combined df: %d' % len(df))
print('Number of rows in SSHCZO df: %d' % len(df_SSHCZO))


# no3_mol_weight = 62.0049
# no3_mgl = df_SSHCZO['NO3 (µmol/L)'] * no3_mol_weight / 1000
# df_SSHCZO['NO3 (mg/L)'] = no3_mgl
#
# # Cl = 35.4530 g/mo
# cl_mol_weight = 35.4530
# cl_mgl = df_SSHCZO['Cl (µmol/L)'] * cl_mol_weight / 1000
# df_SSHCZO['Cl (mg/L)'] = cl_mgl
#
# # SO4 = 96.0626 g/mo
# SO4_mol_weight = 96.0626
# SO4_mgl = df_SSHCZO['SO4 (µmol/L)'] * SO4_mol_weight / 1000
# df_SSHCZO['SO4 (mg/L)'] = SO4_mgl
#
# # print(df_SSHCZO)
#
# del df_SSHCZO['NO3 (µmol/L)']
# del df_SSHCZO['Cl (µmol/L)']
# del df_SSHCZO['SO4 (µmol/L)']

weights = {
            'NO3':62.0049,
            'Cl':35.4530,
            'SO4': 96.0626,
            'Si':28.08550,
            'Al':26.9815386,
            'H':1.007940,
            'NH4': 18.03846,
            'Ca':40.078,
            'Sr':87.6200,
            'Mg':24.30500,
            'Mn':54.9380450,
            'Fe': 55.8450,
            'Na':22.989769280,
            'K':39.09830,
            'PO4': 94.9714,
            'Alkalinity': 100.0869,
            'cation sum':0,
            'anion sum':0,
            'charge balance':0,
            'total N':14.0067,
            'total dissolved N':14.0067,
            'particulate N':14.0067,
            'dissolved org N':14.0067,
            'inorganic N':14.0067,
            'total P':30.9737620,
            'total dissolved P':30.9737620,
            'particulate P':30.9737620,
            'dissolved org P':30.9737620,
            'inorganic P':30.9737620,
            'delta 18O':0,
            'delta 18O std dev':0,
            'delta D':0,
            'delta D std dev':0,
            'D excess':0,
            'tritium':0,
            'tritium std dev':0,
            'total org C':12.0107, #total suspended sediment
            'dissolved org C':12.0107, #dissolved inorganic C
            'particulate org C':12.0107,
           }

mol_units = {
            'NO3':'umol/L',
            'Cl':'umol/L',
            'SO4': 'umol/L',
            'Si':'umol/L',
            'Al':'umol/L',
            'H':'umol/L',
            'NH4': 'umol/L',
            'Ca':'umol/L',
            'Mg':'umol/L',
            'Mn':'umol/L',
            'Fe': 'umol/L',
            'Na':'umol/L',
            'K':'umol/L',
            'Sr': 'umol/L',
            'PO4': 'umol/L',
            'Alkalinity': 'mmol/L CaCO3',
            'cation sum': 'None',
            'anion sum': 'None',
            'charge balance': 'None',
            'total N': 'umol/L',
            'total dissolved N': 'umol/L',
            'particulate N': 'umol/L',
            'dissolved org N': 'umol/L',
            'inorganic N': 'umol/L',
            'total P': 'umol/L',
            'total dissolved P': 'umol/L',
            'particulate P': 'umol/L',
            'dissolved org P': 'umol/L',
            'inorganic P': 'umol/L',
            'delta 18O': 'None',
            'delta 18O std dev':'None',
            'delta D': 'None',
            'delta D std dev': 'None',
            'D excess': 'None',
            'tritium': 'None',
            'tritium std dev': 'None',
            'total org C': 'None',
            'dissolved org C': 'None',
            'particulate org C': 'None',


}

df_SSHCZO= ut.reformat_columns_from_meq_SSH(df_SSHCZO,weights,mol_units)
del df_SSHCZO['IGSN (na)']


dfs = [df, df_SSHCZO]
print('here here')
print(df_SSHCZO.columns)
df_SSHCZO['analyzed material']=df_SSHCZO['ANALYZED MATERIAL (na)']
del df_SSHCZO['ANALYZED MATERIAL (na)']
df_SSHCZO['DO (%)']= df_SSHCZO['DO (%) (%)']
df = pandas.concat(dfs, join='outer') # outer

pickle_out_file = 'sshczo.pkl'
df_SSHCZO.to_pickle(pickle_out_file)

csv_out_file = 'cjczo-lczo-ssczo-sshczo-agg.csv'
df.to_csv(csv_out_file)
#print(df.describe())
print('Number of rows in combo df: %d' % len(df))

#BCCZOGreenLakes.csv


f = open('BCCZOGreenLakes.csv','r')
reader = csv.reader(f)
i = 0
site_code_to_name = {'GREEN LAKE 5': 'GREEN_LAKE_5_','GREEN LAKE 5 ROCK GLACIER': 'GREEN_LAKE_5_ROCK_GLACIER_',
                     'GREEN LAKE 1': 'GREEN_LAKE_1_',
                     'GREEN LAKE 4': 'GREEN_LAKE_4_', }

with open('BCCZOGreenLakes_site_codes_updated.csv', 'w',newline='') as file:
    writer = csv.writer(file,delimiter=',')
    for row in reader:
        i += 1
        if i > 1:
            # print(row[1])
            if row[1] in site_code_to_name:
                site_name = site_code_to_name[row[1]]
                row[1] = "BCCZO_" + site_name
                writer.writerow(row)
            if row[2] == 'DNS':
                row[3] = row[1]
                writer.writerow(row)
            else:
                tmptime = row[2]
                if len(tmptime) == 4:
                    tmptime = tmptime[:2] + ":" + tmptime[2:4]
                if len(tmptime) == 3:
                    tmptime = tmptime[:1] + ":" +tmptime[1:3]
                row[3] = row[1] + " " + tmptime
                writer.writerow(row)
        else:
            writer.writerow(row)


f = 'BCCZOGreenLakes_site_codes_updated.csv'
print('Processing: %s' % f)
indexcols =[0,4,3]
# read these data into a pandas dataframe.  Use "DateTime" as the index column
cols, units, types, methods = extract_df_info_Luquillo(f)
parse_dates = [cols[3]]
dtypes = dict(zip(cols, types))

# read each file into a temporary dataframe
df_BCCZO = pandas.read_csv(f, sep=',', names=cols, dtype=dtypes,
                          parse_dates=parse_dates, na_values=['NP', 'DNS', 'u',' u', '<0.28',
                                                              '<0.161','ND','trace','EQCL'],
                          skiprows=1, index_col=indexcols)

weights = {
            'NO3':62.0049,
            'Cl':35.4530,
            'SO4': 96.0626,
            'Si':28.08550,
            'H':1.007940,
            'NH4': 18.03846,
            'Ca':40.078,
            'Mg':24.30500,
            'Na':22.989769280,
            'K':39.09830,
            'PO4': 94.9714,
            'cation sum':0,
            'anion sum':0,
            'charge balance':0,
            'total N':14.0067,
            'total dissolved N':14.0067,
            'particulate N':14.0067,
            'dissolved org N':14.0067,
            'inorganic N':14.0067,
            'total P':30.9737620,
            'total dissolved P':30.9737620,
            'particulate P':30.9737620,
            'dissolved org P':30.9737620,
            'inorganic P':30.9737620,
            'delta 18O':0,
            'delta 18O std dev':0,
            'delta D':0,
            'delta D std dev':0,
            'D excess':0,
            'tritium':0,
            'tritium std dev':0,
            'total org C':12.0107,
            'dissolved org C':12.0107,
            'particulate org C':12.0107,
           }

mol_units = {
            'NO3': 'ueq/L',
            'Cl': 'ueq/L',
            'SO4':'ueq/L',
            'Si':'umol/L',
            'H': 'ueq/L',
            'NH4':'ueq/L',
            'Ca':'ueq/L',
            'Mg':'ueq/L',
            'Na':'ueq/L',
            'K': 'ueq/L',
            'PO4': 'ueq/L',
            'cation sum': 'None',
            'anion sum': 'None',
            'charge balance': 'None',
            'total N': 'umol/L',
            'total dissolved N': 'umol/L',
            'particulate N': 'umol/L',
            'dissolved org N': 'umol/L',
            'inorganic N': 'umol/L',
            'total P': 'umol/L',
            'total dissolved P': 'umol/L',
            'particulate P': 'umol/L',
            'dissolved org P': 'umol/L',
            'inorganic P': 'umol/L',
            'delta 18O': 'None',
            'delta 18O std dev':'None',
            'delta D': 'None',
            'delta D std dev': 'None',
            'D excess': 'None',
            'tritium': 'None',
            'tritium std dev': 'None',
            'total org C': 'None',
            'dissolved org C': 'None',
            'particulate org C': 'None',


}
#print("HERE")
#changed this value in the file so that the correct type for the column would be selected
# change it back to NaN here.
# print(df_BCCZO.iloc[0]['inorganic P (umol/L)'])
#FIX ME
#df_BCCZO.set_value('GREEN LAKE 5',1500,'inorganic P (umol/L)','NaN') #.iloc[0]['inorganic P (umol/L)'] = 'NaN'
df_BCCZO.iloc[0]['inorganic P (umol/L)'] = 'NaN'
# print(df_BCCZO.iloc[0]['inorganic P (umol/L)'])
#df_BCCZO.set_value('GREEN LAKE 5',1500,'PO4--- (ueq/L)','NaN') # .iloc[0]['PO4--- (ueq/L)'] = 'NaN'
df_BCCZO.iloc[0]['PO4--- (ueq/L)'] = 'NaN'
#print(df_BCCZO.iloc[0]['inorganic P (umol/L)'])
df_BCCZO=ut.reformat_columns_from_meq(df_BCCZO, weights, mol_units)
#print(df_BCCZO.describe())
# print('here here')
print(df_BCCZO.columns)
df_BCCZO['delta Oxygen-18 (per mill)'] = df_BCCZO['delta 18O (per mill)']
df_BCCZO['delta Oxygen-18 std dev'] = df_BCCZO['delta 18O std dev']
df_BCCZO['Delta Deuterium (per mill)'] = df_BCCZO['delta D (per mill)']
df_BCCZO['Delta Deuterium std dev'] = df_BCCZO['delta D std dev']
df_BCCZO['Deuterium excess (per mill)'] = df_BCCZO['D excess (per mill)']

del df_BCCZO['D excess (per mill)']
del df_BCCZO['delta 18O std dev']
del df_BCCZO['delta 18O (per mill)']
del df_BCCZO['delta D (per mill)']
del df_BCCZO['delta D std dev']

dfs = [df, df_BCCZO]

pickle_out_file = 'bcczo.pkl'
df_BCCZO.to_pickle(pickle_out_file)

dfs = [df, df_BCCZO]
df = pandas.concat(dfs, join='outer') # outer

csv_out_file = 'cjczo-lczo-ssczo-sshczo-bcczo-agg-includeall.csv'
df.to_csv(csv_out_file)
#print(df.describe())
#print('Number of rows in combo df: %d' % len(df))


def extract_df_info_ERCZO(csv_file):
    header = []
    units = []
    method = []

    # get the header and datatypes
    with open(csv_file, 'rb') as f:
        lines = f.readlines()
        header = lines[0].strip().decode().split(',')
        data = lines[1].strip().decode().split(',')
    types = [ut.get_type(v) for v in data]

    # reformat the column names
    colnames = []
    unit_labels = []
    for i in range(len(header)):
        unit_label = header[i][header[i].find("("):header[i].find(")")+1]
        unit_labels.append(unit_label)
        print(header[i])
        print(unit_label)
        u = unit_label if unit_label != '' else 'na'
        unit_label = u
        colnames.append(header[i])

    return colnames, unit_labels, types, method

f = 'ERMethodsAndResultsMerged.csv'
print('Processing: %s' % f)
indexcols =[4,1,47]
# read these data into a pandas dataframe.  Use "DateTime" as the index column
cols, units, types, methods = extract_df_info_ERCZO(f)
parse_dates = [cols[47]]
dtypes = dict(zip(cols, types))

# read each file into a temporary dataframe
df_ERCZO = pandas.read_csv(f, sep=',', names=cols, dtype=dtypes,
                          parse_dates=parse_dates, na_values=['-9999', 'NA', 'BAD READING', 'ND'],
                          skiprows=1, index_col=indexcols)
print('Number of rows in temp DF: %d' % len(df_ERCZO))
#https://environmentalchemistry.com/yogi/periodic/Ca-pg2.html
weights = {'Li7':7.016,
           'B11':11.0093,
           'Ca43':42.9588,
           'Rb85':84.9118,
           'Sr88':87.9056,
           'In115':	114.9039,
           'Na23':22.9898,
           'Mg25':24.9858,
           'Al27':26.9815,
           'Si28':27.9769,
           'P31':30.9738,
           'Ca44':43.9555,
           'Mn55':54.9381,
           'Fe56':55.9349,
           'Fe57':56.9354,
           'Co59':58.9332,
           'Ni60':59.9308,
           'Cu63':62.9296,
           'Cu65':64.9278,
           'Cu66':65.9289,
           'Zn66':65.926,
           'Zn64':63.9292,
           'Mo98':97.9054,
           'Ba138':137.9052,
           'K39':38.9637,
           'nM':0,
           'M':0,
           'n':0,
           'Sa':0,
           'Sample id':0,
           'descp no':0,
           'DateTime':0,
           'date': 0,
           'time':0,
           'year':0,
           'month':0,
           'day':0,
           'hour':0,
           'minute':0,
           'filter pore-size':0,
           'method':0,
           'method No.':0,
           }

mol_units = {
           'B11':'ug/L',
           'Ca43':'ug/L',
           'Ca44':'ug/L',
           'Rb85':'ng/L',
           'Sr88':'ng/L',
           'In115':'ng/L',
           'Na23':'ug/L',
           'Mg25':'ug/L',
           'Al27': 'mg/L',
           'Si28':'ug/L',
           'Li7':'ug/L',
           'P31':'ug/L',
           'Mn55':'ug/L',
           'Fe56':'ug/L',
           'Fe57': 'ug/L',
           'Co59':'ug/L',
           'Ni60':'ug/L',
           'Cu66':'ug/L',
           'Cu65':'ug/L',
           'Cu63':'ug/L',
           'Zn66':'ug/L',
           'Zn64':'ug/L',
           'Mo98':'ng/L',
           'Ba138':'ng/L',
           'K39':'ug/L',
           'nM':0,
           'M':0,
           'n':0,
           'Sa':0,
           'Sample id':0,
           'descp no':0,
           'DateTime':0,
           'date':0,
           'time':0,
           'year':0,
           'month': 0,
           'day': 0,
           'hour': 0,
           'minute': 0,
           'filter pore-size': 0,
           'method': 0,
           'method No.': 0,
           }
i=0
for col in df_ERCZO.columns:
    if col == 'Li7 (nM )':
        print('HERE HERE')
        print(col)
    i+=1
    weight = 0
    real_mol_name = ''
    real_unit = ''
    real_weight = 0
    if i >9:
        molecules = col.split('(')#re.findall(r"[a-zA-Z][a-zA-Z]\d{2}", col)
        molecule = molecules[0]
        #count = len(molecules)
        print(molecules)

        molecule = molecule.strip()
        weight = weights[molecule]
        unit = mol_units[molecule]
        if not weight ==0:
            print(molecule + ": " + str(weight))
            print(unit)
            real_mol_name = molecule
            real_unit = unit
            real_weight = weight
        new_col_name = real_mol_name + " (" + real_unit + ")"
        # print(new_col_name)
        if real_unit == 'mg/L':
            # print(df_ERCZO[col].head())
            # print(real_weight)
            mgL = df_ERCZO[col] * real_weight / 1000000.0
            df_ERCZO[new_col_name] = mgL
            del df_ERCZO[col]
        if real_unit == 'ug/L':
            #if col == 'Li7 (nM )':
            print("here")
            print(col)
            print(df_ERCZO[col].head())
            print(real_weight)
            ugL = df_ERCZO[col] * real_weight / 1000.0
            df_ERCZO[new_col_name] = ugL
            del df_ERCZO[col]
        if real_unit == 'ng/L':
            ngL = df_ERCZO[col] * real_weight / 1.0
            df_ERCZO[new_col_name] = ngL
            del df_ERCZO[col]

pickle_out_file = 'erczo.pkl'
df_ERCZO.to_pickle(pickle_out_file)
#print(df_ERCZO.describe())
dfs = [df, df_ERCZO]
df = pandas.concat(dfs, join='outer') # outer



df.index = pd.MultiIndex.from_tuples(df.index,
                                     names=['SiteCode', 'SampleCode','DateTime'])

del df['']

csv_out_file = 'cjczo-lczo-ssczo-sshczo-bcczo-erczo-agg-includeall.csv'
df.to_csv(csv_out_file)