import pandas
import matplotlib.pyplot as plt
from dateutil import parser
import csv
import utilities as ut
# preprocess files
f = open('New_AZ_streamChem.csv','r')
reader = csv.reader(f)
i=0
with open('New_AZ_streamChem_site_codes_updated.csv', 'w',newline='') as file:
    writer = csv.writer(file,delimiter=',')
    for row in reader:
        i += 1
        if i > 4:
            # print(row[1])
            row[1] = "CJCZO_Catalina_Marshall_Gulch_" + row[1]
            writer.writerow(row)
        elif i == 1:
            # Clear MST so it doesn't propogate to other CZOs
            row[0] = ''
            writer.writerow(row)
        else:
            writer.writerow(row)

f.close()

files = ['CJCZO - jemez - NM_StreamWater_Chem_2011.csv','CJCZO - jemez - NM_StreamWater_Chem_2012.csv',
         'CJCZO - jemez - NM_StreamWater_Chem_2013.csv','CJCZO - jemez - NM_StreamWater_Chem_to2010.csv',]
for readf in files:

    f = open(readf, 'r')
    reader = csv.reader(f)
    i = 0
    with open(readf.split('.')[0] + '_site_codes_updated.csv', 'w',newline='') as file:
        writer = csv.writer(file, delimiter=',')
        for row in reader:
            i += 1
            if i > 4:
                # print(row[1])
                row[1] = "CJCZO_Jemez_" + row[1]
                writer.writerow(row)
            elif i == 1:
                #Clear MST so it doesn't propogate to other CZOs
                row[0] = ''
                writer.writerow(row)
            else:
                writer.writerow(row)

    f.close()




def extract_df_info(csv_file):
    header = []
    units = []
    method = []

    # get the header and datatypes
    with open(csv_file, 'rb') as f:
        lines = f.readlines()
        header = lines[0].strip().decode().split(',')
        unit_labels = lines[1].strip().decode("windows-1252").split(',')
        method = lines[3].strip().decode().split(',')
        data = lines[4].strip().decode().split(',')
    types = [ut.get_type(v) for v in data]

    # reformat the column names
    colnames = []
    for i in range(len(header)):
        u = unit_labels[i] if unit_labels[i] != '' else 'na'
        unit_labels[i] = u
        colnames.append('%s (%s)' % (header[i], u))

    return colnames, unit_labels, types, method



# list all files that match to following criteria and store the result in a Python object:
#  1. Jemez CZO
#  2. *.csv file
#  3. has a date in the file name
files = ['CJCZO - jemez - NM_StreamWater_Chem_2011_site_codes_updated.csv','CJCZO - jemez - NM_StreamWater_Chem_2012_site_codes_updated.csv',
         'CJCZO - jemez - NM_StreamWater_Chem_2013_site_codes_updated.csv','CJCZO - jemez - NM_StreamWater_Chem_to2010_site_codes_updated.csv','New_AZ_streamChem_site_codes_updated.csv']

for f in files:
    print(f)

dfs = []
indexcols =[1,2,0]
for f in files:
    print('Processing: %s' % f)

    # read these data into a pandas dataframe.  Use "DateTime" as the index column
    cols, units, types, methods = extract_df_info(f)
    parse_dates = [cols[0]]
    dtypes = dict(zip(cols, types))

    # read each file into a temporary dataframe
    df_temp = pandas.read_csv(f, sep=',', names=cols, dtype=dtypes,
                              parse_dates=parse_dates, na_values=['-9999', 'missing', 'BAD READING'],
                              skiprows=4, index_col=indexcols)
    print('Number of rows in temp DF: %d' % len(df_temp))
    dfs.append(df_temp)



dflarge = pandas.concat(dfs)
df = pandas.concat(dfs, join='inner')
print('Number of rows in DF: %d' % len(dflarge))
print('Number of rows in DF small: %d' % len(df))
print(dflarge.columns)
# indexes = 'SiteCode (na)','DateTime (MST)',
columns = ['NO3 (umoles/L)','F (umoles/L)','Cl (umoles/L)','NO2 (umoles/L)',
                      'Br (umoles/L)','SO4 (umoles/L)','PO4 (umoles/L)',
                      'NO3 (mg/L)','F (mg/L)','Cl (mg/L)','NO2 (mg/L)',
                      'Br (mg/L)','SO4 (mg/L)','PO4 (mg/L)']
new = dflarge.filter(columns, axis=1)

#indexes = ['SiteCode (na)','DateTime (MST)']
#df.set_index(indexes, inplace=True)
#new.set_index(indexes, inplace=True)

df =df.join(new, how='outer') #on=indexes,
print('Number of rows in DF small: %d' % len(df))
print(df.head())
# df['NO3 (umoles/L)'] = dflarge['NO3 (umoles/L)']
# df['F (umoles/L)'] = dflarge['F (umoles/L)']
# df['Cl (umoles/L)'] = dflarge['Cl (umoles/L)']
# df['NO2 (umoles/L)'] = dflarge['NO2 (umoles/L)']
# df['Br (umoles/L)'] = dflarge['Br (umoles/L)']
# df['SO4 (umoles/L)'] = dflarge['SO4 (umoles/L)']
# df['PO4 (umoles/L)'] = dflarge['PO4 (umoles/L)']
#
# df['NO3 (mg/L)'] = dflarge['NO3 (mg/L)']
# df['F (mg/L)'] = dflarge['F (mg/L)']
# df['Cl (mg/L)'] = dflarge['Cl (mg/L)']
# df['NO2 (mg/L)'] = dflarge['NO2 (mg/L)']
# df['Br (mg/L)'] = dflarge['Br (mg/L)']
# df['SO4 (mg/L)'] = dflarge['SO4 (mg/L)']
# df['PO4 (mg/L)'] = dflarge['PO4 (mg/L)']



# perform unit conversion

#Al27 (ug/L) to mg/L
Al27_mgl = df['Al27 (ug/L)']  / 1000
df['Al27 (mg/L)'] = Al27_mgl

Si28_mgl = df['Si28 (ug/L)']  / 1000
df['Si28 (mg/L)'] = Si28_mgl

#http://www.webqc.org/molecular-weight-of-NO3.html

no3_mol_weight = 62.0049
no3_mgl = df['NO3 (umoles/L)'] * no3_mol_weight / 1000
df['NO3 (mg/L)'] = df['NO3 (mg/L)'].combine_first(no3_mgl)

# in g/mol: 18.9984 g/mo
f_mol_weight = 18.9984
f_mgl = df['F (umoles/L)'] * f_mol_weight / 1000
df['F (mg/L)'] = df['F (mg/L)'].combine_first(f_mgl)

# Cl = 35.4530 g/mo
cl_mol_weight = 35.4530
cl_mgl = df['Cl (umoles/L)'] * cl_mol_weight / 1000
df['Cl (mg/L)'] = df['Cl (mg/L)'].combine_first(cl_mgl)
# NO2 = 46.00550 g/mo
NO2_mol_weight = 46.00550
NO2_mgl = df['NO2 (umoles/L)'] * NO2_mol_weight / 1000
df['NO2 (mg/L)'] = df['NO2 (mg/L)'].combine_first(NO2_mgl)

# Br = 79.9040 g/mo
Br_mol_weight = 79.9040
Br_mgl = df['Br (umoles/L)'] * Br_mol_weight / 1000
df['Br (mg/L)'] = df['Br (mg/L)'].combine_first(Br_mgl)

# SO4 = 96.0626 g/mo
SO4_mol_weight = 96.0626
SO4_mgl = df['SO4 (umoles/L)'] * SO4_mol_weight / 1000
df['SO4 (mg/L)'] = df['SO4 (mg/L)'].combine_first(SO4_mgl)

# PO4 = 94.9714 g/mo
PO4_mol_weight = 94.9714
PO4_mgl = df['PO4 (umoles/L)'] * PO4_mol_weight / 1000
df['PO4 (mg/L)'] = df['PO4 (mg/L)'].combine_first(PO4_mgl)

print(df.columns)

del df['Br (umoles/L)']
del df['NO3 (umoles/L)']
del df['F (umoles/L)']
del df['Cl (umoles/L)']
del df['NO2 (umoles/L)']
del df['SO4 (umoles/L)']
del df['PO4 (umoles/L)']
del df['Al27 (ug/L)']
del df['Si28 (ug/L)']

temp_df2 = None
firstdfload = True
df['total org C (mg/L)'] = df['TOC (mg/L)']
df['total N (mg/L)']=df['TN (mg/L)']
#TIC (mg/L)
df['total inorganic C (mg/L)']=df['TIC (mg/L)']
del df['TOC (mg/L)']
del df['TN (mg/L)']
del df['TIC (mg/L)']

# write out the file for CJCZO
csv_out_file = 'cjczo-agg.csv'
pickle_out_file = 'cjczo.pkl'

df.to_csv(csv_out_file)

df.to_pickle(pickle_out_file)