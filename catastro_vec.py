# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 16:03:09 2016

@author: fgnovo
"""

import os,sys
import logging
import numpy as np
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
   if x.apply(lambda x: x.count())[0] > 1 : 
       return(x.sort_values('altura').altura.diff()) 
   else:
       return(x.altura) 
    
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
logger = logging.getLogger(__name__)
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
    df_dif = df
    df_dif = df_dif[df_dif['altura'] > 0]
    logger.info("Calculando diferencia de alturas")
    df_dif = df.groupby('FID').apply(dif_altura)
    df_dif = df_dif.reset_index()
    df_dif = df_dif[['level_1','altura']]
    df_dif.columns = ['Pindex','dif_altura']
    logger.info("Unniendo diferencia de alturas a la tabla")
    df2 = pd.merge(df,df_dif,left_index=True,right_on='Pindex',how='left')
    df2 = df2.fillna(0)
    df2['area_fachada'] = df2['longitud']*3.0*df2['dif_altura']
    df2['fachada_total'] = df2['perimetro']*3.0*df2['altura']
    df_sup_pol = pd.DataFrame(df2.groupby('FID_CONSTR').area_fachada.sum())
    df_sup_pol.rename(columns={'area_fachada':'afachada_pol'}, inplace=True)
    df2 = pd.merge(df2,df_sup_pol,left_on='FID_CONSTR',right_index = True,how='left')
    df2['area_medianeras'] = df2['fachada_total']-df2['afachada_pol']
    df2 = df2[['FID_CONSTR','ref_cat','altura','afachada_pol','fachada_total','area_medianeras']]
    df2 = df2.drop_duplicates(subset='FID_CONSTR')
    logger.info("Iniciando extracción fichero .cat")
    dfcat = cat.extraer_inf_cat('./data_cat/'+nfile[1])
    logger.info("Uniendo información vectorial y .cat")
    df_out=pd.merge(df2,dfcat,left_on="ref_cat",right_index = True, how='left')
    df_out = df_out.fillna(0)
    df_out = df_out[['FID_CONSTR','altura','afachada_pol','fachada_total','area_medianeras','year_const','preservation','tipology','year_reform']]
    df_out.altura = df_out.altura.astype(int)
    df_out.year_const = df_out.year_const.astype(int)
    df_out.preservation = df_out.preservation.astype(int)
    df_out.tipology = df_out.tipology.astype(int)
    df_out.year_reform = df_out.year_reform.astype(int)

    df_out = df_out.round({'afachada_pol':2,'fachada_total':2,'area_medianeras':2})
    df_out.to_csv('./output_data/'+concello+'.csv')
print('FINAL')
logger.info('SCRIPT FINALIZADO')
    