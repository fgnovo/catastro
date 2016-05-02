# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 16:03:09 2016

@author: fgnovo
"""

import os
import logging
import logging.config
import pandas as pd
from simpledbf import Dbf5
import xml.etree.ElementTree as ET
import libcatastrocat2 as cat

#**************************************************************
#FUNCIÓN PARA PASAR NÚMEROS ROMANOS A ENTEROS
#***************************************************************
# numero_romano debe ser un String, si el string no representa un número
#devuelve -1
def romano_a_arabigo(numero_romano):
    # Resultado de la transformacion
    numero_romano.upper()
    resultado = 0

    # Usamos un diccionario, ya que se adapta al concepto
    # de que a cada letra le corresponde un valor
    valores = {
        'M' : 1000,
        'D' : 500,
        'C' : 100,
        'L' : 50,
        'X' : 10,
        'V' : 5,
        'I' : 1
    }

    if len(numero_romano) > 0:
        # Con esto, siempre sumamos el primer numero
        valor_anterior = numero_romano[0]
    else: return -1 

    # Por cada letra en el numero romano (string)
    for letra in numero_romano:

        # Si la letra se encuentra en el diccionario
        if letra in valores:
            # Obtenemos su valor
            valor_actual = valores[letra]
        else:
            # Si no, la letra es invalida
            #print 'Valor invalido:', letra
            logger.debug('Número no valido {}'.format(numero_romano))
            return -1

        # Si el valor anterior es mayor o igual que el
        # valor actual, se suman
        if valor_anterior >= valor_actual:
            resultado += valor_actual
        # Si no, se restan
        else:
            # Esto equivale a:
            # resultado = (resultado - valor_anterior) + (valor_actual - valor_anterior)
            resultado += valor_actual - (2 * valor_anterior)

        # El valor actual pasa a ser el anterior, para analizar
        # la siguiente letra en el numero romano
        valor_anterior = valor_actual

    # Al terminar, retorna el numero resultante
    return resultado
#**************************************************************
#FUNCIÓN PARA PASAR STRING A NÚMEROS CAMPO CONSTRU
#***************************************************************
#los valores ENT T Y AT suman meida planta en altura
def valor_altura(altura):
    #elementos que suman 0.5
    integrados = ['VOL','TZA']
    for substring in integrados:
        if substring in altura:
            altura = altura.replace(substring,'')
    if altura in media_planta:
        return(0.5)
    else:
        return(romano_a_arabigo(altura))
#**************************************************************
#FUNCIÓN PARA PASAR STRING CAMPO CONSTRU A LA ALTURA DEL POLÍGONO
#***************************************************************
#los valores ENT T Y AT suman meida planta en altura
def procesar_altura(constru):
    #elimino los espacios en blanco
    constru.strip()
    #paso todas las letras a mayúscula
    constru.upper()
    lista_alturas=[]
    maxima_altura = 0
    #si el string es vacio devuelvo altura 0
    if len(constru)>0:
        #separo por símbolo +
        alturas = constru.split('+')
        #print(alturas)
        #si la primera letra del primer elemento del array es -1 lo elimino
        if(alturas[0]=='-'):
             del alturas[0]
        primero = True
        #creo una lista con todos los valores del campo CONSTRU validos
        for altura in alturas:
            v_altura = valor_altura(altura)
            #si valor_altura devuelve valor negativo se elimna campo
            if v_altura >=0:
                if primero:
                    lista_alturas =[v_altura]
                    primero = False
                else:
                    lista_alturas.append(v_altura)
        #si la lista no está vacia
        if lista_alturas != []:
            #cuento cuantos unos y ceros  hay en la lista
            numeros_uno = lista_alturas.count(1)
            numeros_cero = lista_alturas.count(0)
            #selecciono el número mayor
            maxima_altura = max(lista_alturas)
            #si es bajo con bajo cubierta  la altura es o
            if (numeros_cero == 1) and (numeros_uno == 1):
                maxima_altura = 0
            #sumo los elemento 0.5            
            numeros_medios = lista_alturas.count(0.5)
            maxima_altura += (numeros_medios/2)
        else:#no numeros validos altura
            maxima_altura = 0
        return(maxima_altura)
    else:# string vacio
        return(maxima_altura)
        
#**********************
def configuracion():
    codigos_a_eliminar = set()
    media_planta = set()
    integrados = set()
    files = {}
    tree = ET.parse('config.xml')
    root = tree.getroot()
    for ipath in root.findall('path'):
        path= ipath.text
    for item in root.findall('codigos_eliminar/item'):
        codigos_a_eliminar.add(item.text)
    for item in root.findall('media_planta/item'):
        media_planta.add(item.text)
    for item in root.findall('integrados/item'):
        integrados.add(item.text)
    for ifile in root.findall('listado_archivos/concello'):  
        files.update({ifile.get('nombre'):(ifile.find('avectorial').text,ifile.find('acat').text)})
    return(path,codigos_a_eliminar,media_planta,integrados,files)

#************************************
#CALCULAR DIFERENCIA DE ALTURAS
#*************************************
def dif_altura(x):
    x['dif_altura'] =x.sort_values('altura').altura.diff()
    if x.ref_cat.min() == x.ref_cat.max():
        x['interior'] = x.altura.min()
    return(x)
#*************************************
#ELIMINO LOS POLIGONOS AISLADOS CON USO VIVIENDA CON MÁS DE UN POLÍGONO POR REF. CATASTRAL 
#de altura = 1 y con algún polígono de altura>1
#**************************************
def tipo_zero(x):
    if (x.altura == 1) and (x.area_medianeras<0.001):
        x.tipology = 0
        logger.debug('Eliminar aislado{}:'.format(x))
    return(x)
    
def del_aislados(x):
    if ((x.count().FID_CONSTR> 1) and (x.altura.max() >1)):
        return(x.apply(tipo_zero,axis = 1))
    else:
        return(x)
        
#***********************************************
#Programa principal
#***********************************************

path,codigos_a_eliminar,media_planta,integrados,files = configuracion()#"/Users/fgnovo/workspace/python-apec/data/c"
os.chdir(path)
#(file for file in os.listdir(path) if os.path.isfile(file))
if not (os.path.exists("output_data")):
    os.mkdir("output_data")
if not (os.path.exists("log_data")):
    os.mkdir("log_data")
#INICIO DE LOG
logger = logging.getLogger('catastro')
logger.setLevel(logging.DEBUG)
#create a file handler and console
fh = logging.FileHandler('./log_data/catastro.log')
fh.setLevel(logging.DEBUG)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')
fh.setFormatter(formatter)
#console.setFormatter(formatter)
logger.addHandler(console)
logger.addHandler(fh)
logger.info('INICIANDO PROCESADO FICHERO DBF')
for concello,nfile in files.iteritems():
    logger.info("Concello : {}".format(concello))
    logger.info("Iniciando el procesado del fichero: {}".format(nfile[0]))
    #abre fichero dbf y lo convierte a pandas
    dbf = Dbf5('./data_vec/'+nfile[0],codec = 'utf-8') 
    df = dbf.to_dataframe()
    #cambio el nombre de la primera columna por FID
    df=df.rename(columns = {df.columns[0]:'FID'})
    #elimino los usos no necesarios 
    df = df[df.CONSTRU.isin(codigos_a_eliminar) == False]
    #recoloco los indices
    df.index = range(len(df))
    df.altura = 0.0
    index = 0
    for alturas in df.CONSTRU:
        logger.debug('Iniciando procesado línea {}'.format(index))
        a =  procesar_altura(alturas)
        logger.debug("Altura: {}:".format(a))
        logger.debug("Alturas string: {}:".format(alturas))
        df.ix[index,'altura'] = a
        index += 1
        if (index % 500 == 0):
            logger.info(" {} Registros procesados".format(index))      
    #elimino los poligonos sin altura
    df_dif = df
    df_dif = df_dif[df_dif['altura'] > 0]
    #me quedo con los campos necesarios
    df_dif = df_dif[['FID','FID_CONSTR','ref_cat','perimetro','longitud','altura']]
    logger.info("Calculando diferencia de alturas")
    #presupongo con no es medianera, es decir es fachada
    df_dif['dif_altura'] = df_dif.altura
    df_dif['interior'] = 0
    #separo las medianeras de las no medianeras
    duplicated = df_dif.FID.duplicated(keep = False)
    df_dif_duplicated = df_dif[duplicated]
    df_dif_not_duplicated = df_dif[~duplicated]
    #calculo de las medianeras que parte es medianera y que parte es fachada
    #detecto las medianeras interiores, sumando su supuerficie en campo interior
    df_dif_duplicated = df_dif_duplicated.groupby('FID').apply(dif_altura)
    df_dif = pd.concat([df_dif_not_duplicated,df_dif_duplicated])
    logger.info("Calculando areas de medianeras y fachada")
    df_dif = df_dif.fillna(0)
    df_dif['area_fachada'] = df_dif['longitud']*3.0*df_dif['dif_altura']
    df_dif['area_interior'] = df_dif['longitud']*3.0*df_dif['interior']
    df_areas = df_dif.groupby('FID_CONSTR').sum()[['longitud','area_fachada','area_interior']]
    df_dif=df_dif.drop(['area_fachada','area_interior','longitud'],axis=1)
    df_dif = pd.merge(df_dif,df_areas,left_on='FID_CONSTR',right_index = True,how='left')
    df_dif = df_dif.drop_duplicates(subset='FID_CONSTR')    
    df_dif['fachada_total'] = df_dif['longitud']*3.0*df_dif['altura']
    df_dif['area_medianeras'] = df_dif['fachada_total']-df_dif['area_fachada']-df_dif['area_interior']
    logger.info("Iniciando extracción fichero .cat")
    dfcat = cat.extraer_inf_cat('./data_cat/'+nfile[1])
    logger.info("Uniendo información vectorial y .cat")
    df_out=pd.merge(df_dif,dfcat,left_on="ref_cat",right_index = True, how='left')
    df_out = df_out.fillna(0)
    df_out = df_out[['FID_CONSTR','altura','area_fachada','fachada_total','area_medianeras','area_interior','year_const','preservation','tipology','year_reform','ref_cat']]
    df_out.altura = df_out.altura.astype(int)
    df_out.year_const = df_out.year_const.astype(int)
    df_out.preservation = df_out.preservation.astype(int)
    df_out.tipology = df_out.tipology.astype(int)
    df_out.year_reform = df_out.year_reform.astype(int)
    df_out = df_out.round({'area_fachada':2,'fachada_total':2,'area_medianeras':2,'area_interior':2})
    #Elimino los poligonos con area_medianeras <0,01 y altura  = 1 y que sea más de un poligono 
    #por ref_cat,tenga uso vivienda y al menos un poligono altura>1
    logger.info('Elimino los poligonos vivienda aislados')  
    #agrupo por referencia catastral y creo un df con index ref_cat
    #FID_CONST y tipolgy == 0 si es a eliminar
    df_out = df_out.groupby('ref_cat').apply(del_aislados)  
    df_out = df_out.reset_index()
    df_out = df_out[df_out.tipology > 0]
    df_out.to_csv('./output_data/'+concello+'_total.csv')
    #Filtro uso vivienda
    df_out = df_out[df_out['tipology']<2]
    df_out.to_csv('./output_data/'+concello+'_vivienda.csv')
logger.info('SCRIPT FINALIZADO')
    