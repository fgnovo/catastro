import struct
import os,sys
import numpy as np
import pandas as pd


# This function transform a string in an array, has two parameters: 
# ctype:Register type (could be 13 or 14)
# line: String with fixed length 998, catastro register
# Return if ctype = 13 an array of strings with two fields: Parcela catastral and Year of construction
# if ctype = 14 return an array with 7 fields:Parcela Catastral,Floor,DGC Code, Type or reform or rehabilitation,
# Year of reform, First four digits Tipologia Constructiva,building state of preservation,under cover
def line_array(line,ctype):
    if ctype == 13:
        fieldwidths_type = (-28,14,-251,4)
    else :
        fieldwidths_type = (-28,14,-20,3,-3,3,1,4,-26,4,1)
    fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's') for fw in fieldwidths_type)
    fieldstruct = struct.Struct(fmtstring)
    parse = fieldstruct.unpack_from
    fields = list(parse(line))
    if ctype ==13:
        try:
            fields[1] = int(fields[1])
        except ValueError:
            print ("{0} Not number year of construction in register: }{1}".format(fields[0],fields[1]))
            return([])
    else:
        if (fields[1] == '+1 ') and (ctype == 14):
            fields[1] = '-50'
        elif (fields[1] == 'SM ') and (ctype == 14):
            fields[1] = '-51'
        elif (fields[1] == 'AT ') and (ctype == 14):
            fields[1] = '-49'
        elif (fields[1] == 'EN ') and (ctype == 14):
            fields[1] = '-48'
        elif (fields[1] == 'OM ') and (ctype == 14):
            fields[1] = '-47'
        try:
            x = int(fields[1]) 
        except ValueError:
            print ("Not number error in register 14  floor {} ".format(fields))
            x = -100
        try:
            y = int(fields[4]) 
        except ValueError:
            print ("Not number error in register 14 year {} ".format(fields))
            y = -100
        fields[1] = x
        fields[4] = y
    return(fields)
    
#This function insert an array in a pandas dataframe (df13 or df15),has two parameters:   


def insert_line_df(fields,ctype,df):
    if ctype == 13:
        data = pd.DataFrame({"ref_catastral":[fields[0]],"year_const":[fields[1]]})
    else:
        data = pd.DataFrame({"ref_catastral":[fields[0]],"floor":[fields[1]],"purpose":[fields[2]],"t_reform":[fields[3]],
                    "year_reform":[fields[4]],"tipology":[fields[5]],"preservation":[fields[6]]})
    return(df.append(data))
    
    
path = "/Users/fgnovo/workspace/python-apec/data"
os.chdir(path)
files = (file for file in os.listdir(path) if os.path.isfile(file))
if not (os.path.exists("./output_data")):
    os.mkdir("output_data")
for file in files:
    df13 = pd.DataFrame()
    df14 = pd.DataFrame()
    f = open(file,'r', enconding ='latin-1')
    print ("File opened: {0}".format(f.name))
    # read all lines length 1000
    i = 0
    while True:
        ctype = f.read(2)
        line = f.read(998)
        if not line:
            print ("File {} finished".format(file))
            f.close()
            break
        else:
            i += 1
            if( int(ctype)>=13 and int(ctype)<=14):
                fields = line_array(line,int(ctype))
                if fields != []:
                    if int(ctype) == 13:
                        df13 = insert_line_df(fields,13,df13)
                    else:
                        df14 = insert_line_df(fields,14,df14)
                #print ("Type :{0} - {1}".format(ctype,fields))
            if (i % 500 == 0):
                print(" {} Registros procesados".format(i))
    print (len(pd.unique(df13.ref_catastral.ravel())))
    print (len(pd.unique(df14.ref_catastral.ravel())))
    df = pd.merge(df13,df14,on='ref_catastral')
    print (len(pd.unique(df.ref_catastral.ravel())))
    #select only values purpose == V and select de max floor by ref_catastral
    df_v = df[df.purpose == 'V']
    df_grouped = df_v.groupby("ref_catastral")  
    df.to_csv("./output_data/"+file+"_out.csv")

