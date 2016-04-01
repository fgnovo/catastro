
import struct
import numpy as np
import pandas as pd
from scipy.stats.mstats import mode


# This function transform a string in an array, has two parameters: 
# ctype:Register type (could be 13 or 14)
# line: String with fixed length 998, catastro register
# Return if ctype = 13 an array of strings with two fields: Parcela catastral and Year of construction
# if ctype = 14 return an array with 7 fields:Parcela Catastral,Floor,DGC Code, Type or reform or rehabilitation,
# Year of reform, First four digits Tipologia Constructiva,building state of preservation,under cover
def line_array(line,ctype):
    if ctype == 13:
        fieldwidths_type = (-28,14,4,-247,4)
    else :
        fieldwidths_type = (-28,14,-10,4,-6,3,-3,3,1,4,-26,2,2,1)
    fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's') for fw in fieldwidths_type)
    fieldstruct = struct.Struct(fmtstring)
    parse = fieldstruct.unpack_from
    fields = list(parse(line))
    if ctype ==13:
        try:
            fields[2] = int(fields[2])
        except ValueError:
            #print ("{0} Not number year of construction in register: }{1}".format(fields[0],fields[1]))
            return([])
    else:
        if (fields[2] == '+1 ') and (ctype == 14):
            fields[2] = '-50'
        elif (fields[2] == 'SM ') and (ctype == 14):
            fields[2] = '-51'
        elif (fields[2] == 'AT ') and (ctype == 14):
            fields[2] = '-49'
        elif (fields[2] == 'EN ') and (ctype == 14):
            fields[2] = '-48'
        elif (fields[2] == 'OM ') and (ctype == 14):
            fields[2] = '-47'
        try:
            x = int(fields[2]) 
        except ValueError:
            #print ("Not number error in register 14  floor {} ".format(fields))
            x = -100
        try:
            y = int(fields[5]) 
        except ValueError:
            #print ("Not number error in register 14 year {} ".format(fields))
            y = -100
        fields[2] = x
        fields[5] = y
    return(fields)
    
#This function insert an array in a pandas dataframe (df13 or df15),has two parameters:   


def insert_line_df(fields,ctype,df):
    if ctype == 13:
        data = pd.DataFrame({"ref_catastral":[fields[0]],"cuc":[fields[1]],"year_const":[fields[2]]})
    else:
        data = pd.DataFrame({"ref_catastral":[fields[0]],"cuc":[fields[1]],"floor":[fields[2]],"purpose":[fields[3]],"t_reform":[fields[4]],
                    "year_reform":[fields[5]],"tipology":[int(fields[6])],"subtipology":[fields[7]],"preservation":[fields[8]]})
    return(df.append(data))

def select_tipology(x):
    if x['tipology'].min()==1:
        x['tipology'] = 1
    else:
        x['tipology'] = int(mode(x['tipology'])[0][0])
    return(x.iloc[0])

    
def extraer_inf_cat(file):
    #df13 = pd.DataFrame()
    #df14 = pd.DataFrame()
    data13 = []
    data14 = []
    f = open(file,'r')
    print "File opened: {0}".format(f.name)
    # read all lines length 1000
    i = 0
    while True:
        ctype = f.read(2)
        line = f.read(998)
        if not line:
            print "File {} finished".format(file)
            f.close()
            break
        else:
            i += 1
            if( int(ctype)>=13 and int(ctype)<=14):
                fields = line_array(line,int(ctype))
                if fields != []:
                    if int(ctype) == 13:
                        #data13 = insert_line_df(fields,13,df13)
                        if data13 == []:
                            data13 = [fields]
                        else:
                            data13.append(fields)
                    else:
                        #data14 = insert_line_df(fields,14,df14)
                        if data14 == []:
                            data14 = [fields]
                        else:
                            data14.append(fields)
                #print ("Type :{0} - {1}".format(ctype,fields))
            if (i % 500 == 0):
                print(" {} Registros procesados".format(i))

    df13 = pd.DataFrame(data13)
    df13.columns = ["ref_catastral","cuc","year_const"]
    df14 = pd.DataFrame(data14)
    df14.columns = ["ref_catastral","cuc","floor","purpose","t_reform","year_reform","tipology","subtipology","preservation"]    
    df = pd.merge(df13,df14,on=['cuc','ref_catastral'],how="right")
    df.to_csv("salida1.csv")
    dfcat = df.groupby('ref_catastral').apply(select_tipology)
    dfcat[['year_const','preservation','tipology','subtipology','year_reform']].to_csv("salida2.csv")
    return(dfcat[['year_const','preservation','tipology','subtipology','year_reform']])
    
    





    


