import pandas
import matplotlib.pyplot as plt
from dateutil import parser
import csv
import re

def get_type(v):
    try:
        x = float(v)
        return 'float'
    except ValueError:
        pass
    return 'str'


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
    types = [get_type(v) for v in data]

    # reformat the column names
    colnames = []
    for i in range(len(header)):
        u = unit_labels[i] if unit_labels[i] != '' else 'na'
        unit_labels[i] = u
        colnames.append('%s (%s)' % (header[i], u))

    return colnames, unit_labels, types, method

def reformat_columns_from_nM(df, weights, mol_units):
    i = 0
    for col in df.columns:
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
            # if count == 0:
            #     molecules = re.findall(r"[a-zA-Z]\d{2}", col)
            #     count = len(molecules)
            #     print(molecules)
            # if count == 0:
            #     molecules = re.findall(r"[a-zA-Z][a-zA-Z]\d{3}", col)
            #     count = len(molecules)
            # if count == 0:
            #     molecules = re.findall(r"[a-zA-Z]\d{3}", col)
            #for molecule in molecules:
            molecule = molecule.strip()
            weight = weights[molecule]
            unit = mol_units[molecule]
            if not weight ==0:
                print(molecule + ": " + str(weight))
                real_mol_name = molecule
                real_unit = unit
                real_weight = weight
            new_col_name = real_mol_name + " (" + real_unit + ")"
            # print(new_col_name)
            if real_unit == 'mg/L':
                # print(df_ERCZO[col].head())
                # print(real_weight)
                mgL = df[col] * real_weight / 1000000.0
                df[new_col_name] = mgL
                del df[col]
            if real_unit == 'ug/L':
                # print(df_ERCZO[col].head())
                # print(real_weight)
                ugL = df[col] * real_weight / 1000.0
                df[new_col_name] = ugL
                del df[col]
            if real_unit == 'ng/L':
                ngL = df[col] * real_weight / 1.0
                df[new_col_name] = ngL
                del df[col]


def reformat_columns_from_meq(df, weights, mol_units):
    i = 0
    for col in df.columns:
        #print(col)
        i+=1
        weight = 0
        real_mol_name = ''
        real_unit = ''
        real_weight = 0
        if i >7:
            molecules = col.split('(')#re.findall(r"[a-zA-Z][a-zA-Z]\d{2}", col)
            molecule = molecules[0]
            if "+" in molecule:
                molecules = col.split('+')  # re.findall(r"[a-zA-Z][a-zA-Z]\d{2}", col)
                molecule = molecules[0]
            if "-" in  molecule:
                molecules = col.split('-')  # re.findall(r"[a-zA-Z][a-zA-Z]\d{2}", col)
                molecule = molecules[0]
            # molecules = re.findall(r"[a-zA-Z][a-zA-Z]", molecule) #col.split('(')
            #count = len(molecules)
            #print(molecule)
            # if count == 0:
            #      molecules = re.findall(r"[a-zA-Z]", col)
            #      count = len(molecules)
            #      print(molecules)
            # if count == 0:
            #     molecules = re.findall(r"[a-zA-Z][a-zA-Z]\d{3}", col)
            #     count = len(molecules)
            # if count == 0:
            #     molecules = re.findall(r"[a-zA-Z]\d{3}", col)
            #for molecule in molecules:
            molecule = molecule.strip()
            #try:
            weight = weights[molecule]
            unit = mol_units[molecule]
            #print("weight " + str(weight))
            #print("unit " + unit)
            if not weight ==0:
                #print(molecule + ": " + str(weight))
                real_mol_name = molecule
                real_unit = unit
                real_weight = weight
            new_col_name = real_mol_name + " (mg/L)"
            # print(new_col_name)
            if real_unit == 'mg/L':
                # print(df_ERCZO[col].head())
                # print(real_weight)
                mgL = df[col] * real_weight
                df[new_col_name] = mgL
                del df[col]
            if real_unit == 'ueq/L':
                # print(df_ERCZO[col].head())
                # print(real_weight)
                #print(real_weight)
                #print(col)
                #print("new name " + new_col_name)
                ugL = df[col] * real_weight / 1000.0
                df[new_col_name] = ugL
                del df[col]
            if real_unit == 'umol/L':
                #print(df[col].head())
                #print(real_weight)
                #print(col)
                #print("new name " + new_col_name)
                umolL = df[col] * real_weight / 1000.0
                df[new_col_name] = umolL
                del df[col]
            if real_unit == 'umol/L to ug':
                    # print(df[col].head())
                    # print(real_weight)
                    # print(col)
                    # print("new name " + new_col_name)
                    new_col_name = real_mol_name + " (ug/L)"
                    umolL = df[col] * real_weight
                    df[new_col_name] = umolL
                    del df[col]
            #except KeyError:
            #    print(molecule)
    return df


def reformat_columns_from_meq_SSH(df, weights, mol_units):
    i = 0
    for col in df.columns:
        #print(col)
        i+=1
        weight = 0
        real_mol_name = ''
        real_unit = ''
        real_weight = 0
        if i >9:
            molecules = col.split('(')#re.findall(r"[a-zA-Z][a-zA-Z]\d{2}", col)
            molecule = molecules[0]
            if "+" in molecule:
                molecules = col.split('+')  # re.findall(r"[a-zA-Z][a-zA-Z]\d{2}", col)
                molecule = molecules[0]
            if "-" in  molecule:
                molecules = col.split('-')  # re.findall(r"[a-zA-Z][a-zA-Z]\d{2}", col)
                molecule = molecules[0]
            # molecules = re.findall(r"[a-zA-Z][a-zA-Z]", molecule) #col.split('(')
            #count = len(molecules)
            #print(molecule)
            # if count == 0:
            #      molecules = re.findall(r"[a-zA-Z]", col)
            #      count = len(molecules)
            #      print(molecules)
            # if count == 0:
            #     molecules = re.findall(r"[a-zA-Z][a-zA-Z]\d{3}", col)
            #     count = len(molecules)
            # if count == 0:
            #     molecules = re.findall(r"[a-zA-Z]\d{3}", col)
            #for molecule in molecules:
            molecule = molecule.strip()
            #try:
            weight = weights[molecule]
            unit = mol_units[molecule]
            #print("weight " + str(weight))
            #print("unit " + unit)
            if not weight ==0:
                #print(molecule + ": " + str(weight))
                real_mol_name = molecule
                real_unit = unit
                real_weight = weight
            new_col_name = real_mol_name + " (mg/L)"
            # print(new_col_name)
            if real_unit == 'mg/L' : #or real_unit=='mmol/L CaCO3' mmol to mg/L CaCO3 doesn't make sense
                # print(df_ERCZO[col].head())
                # print(real_weight)
                mgL = df[col] * real_weight
                df[new_col_name] = mgL
                del df[col]
            if real_unit == 'ueq/L':
                # print(df_ERCZO[col].head())
                # print(real_weight)
                #print(real_weight)
                #print(col)
                #print("new name " + new_col_name)
                ugL = df[col] * real_weight / 1000.0
                df[new_col_name] = ugL
                del df[col]
            if real_unit == 'umol/L':
                #print(df[col].head())
                #print(real_weight)
                #print(col)
                #print("new name " + new_col_name)
                umolL = df[col] * real_weight / 1000.0
                df[new_col_name] = umolL
                del df[col]
            #except KeyError:
            #    print(molecule)
    return df