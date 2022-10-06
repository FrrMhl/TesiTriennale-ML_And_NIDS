import numpy as np
import pandas as pd
from ipaddress import ip_address
from readAndWrite import splitFromCompleteToSingle, scritturaSuFile

pd.options.mode.chained_assignment = None





"""
    funzione per la modifica delle varie features del dataset completo
    parametri -> datasetcompleto
"""
def preprocessing(completeDataFrame):

    # rimozione valori nulli, traffico non tcp
    completeDataFrame.replace('', np.nan, inplace=True)
    completeDataFrame.dropna(inplace=True)
    completeDataFrame = completeDataFrame[completeDataFrame['Proto'] == 'tcp']

    # creazione nuove features (Dur e DstBytes potrebbero essere 0)
    completeDataFrame['DstBytes'] = completeDataFrame.apply(lambda row: row.TotBytes - row.SrcBytes, axis=1)
    completeDataFrame['BytesPerPkt'] = completeDataFrame.apply(lambda row: row.TotBytes / row.TotPkts, axis=1)
    completeDataFrame['PktsPerSec'] = completeDataFrame.apply(lambda row: row.TotPkts / row.Dur if row.Dur != 0 else np.nan, axis=1)
    maxValue = completeDataFrame['PktsPerSec'].max()
    completeDataFrame['PktsPerSec'].replace(np.nan, maxValue, inplace=True)
    completeDataFrame['RatioOutIn'] = completeDataFrame.apply(lambda row: row.SrcBytes / row.DstBytes if row.DstBytes != 0 else np.nan, axis=1)
    maxValue = completeDataFrame['RatioOutIn'].max()
    completeDataFrame['RatioOutIn'].replace(np.nan, maxValue, inplace=True)

    # eliminazione samples in base al 95-esimo percentile 
    rowPercentile = ['Dur', 'DstBytes', 'SrcBytes', 'TotPkts', 'PktsPerSec']
    for col in rowPercentile:
        val = np.percentile(completeDataFrame[col], 95)
        completeDataFrame = completeDataFrame[completeDataFrame[col] < val]

    # creazione features per ip e porte per evitare overfitting
    ipNew = ['IPSrcType', 'IPDstType']
    ipOld = ['SrcAddr', 'DstAddr']
    for (x, y) in zip(ipNew, ipOld):
        fn = lambda row: 1 if ip_address(row[y]).is_private else 0
        completeDataFrame[x] = completeDataFrame.apply(fn, axis=1)

    portOld = ['Sport', 'Dport']
    portNewSorg = ['SrcPortWellKnown', 'SrcPortRegistered', 'SrcPortPrivate']
    portNewDest = ['DstPortWellKnown', 'DstPortRegistered', 'DstPortPrivate']
    for x in portOld:
        fn = [lambda row: 1 if 0 <= int(row[x]) <= 1023 else 0,
              lambda row: 1 if 1024 <= int(row[x]) <= 49151 else 0,
              lambda row: 1 if int(row[x]) >= 49152 else 0]
        if x == 'Sport':
            for (y, z) in zip(portNewSorg, fn):
                completeDataFrame[y] = completeDataFrame.apply(z, axis=1)
        else:
            for (y, z) in zip(portNewDest, fn):
                completeDataFrame[y] = completeDataFrame.apply(z, axis=1)

    # eliminazione colonne inutili e modifica dei dtype
    renameColumn = {
        'Dur': 'Duration',
        'Proto': 'Protocol',
        'Dir': 'Direction',
        'sTos': 'SrcToS',
        'dTos': 'DstToS',
        'SrcBytes': 'OutBytes',
        'DstBytes': 'InBytes'}
    completeDataFrame.rename(columns=renameColumn, inplace=True)
    completeDataFrame.drop(columns=['StartTime', 'SrcAddr', 'Sport', 'DstAddr', 'Dport'], inplace=True)

    completeDataFrame['Direction'] = completeDataFrame['Direction'].astype('category')
    
    # codifica colonne categoriche 
    encoded = pd.get_dummies(completeDataFrame['Direction'], prefix='Direction')
    completeDataFrame = pd.concat([completeDataFrame, encoded], axis=1)
    completeDataFrame.drop(columns='Direction', inplace=True)

    completeDataFrame['Protocol'].replace({'tcp': 0}, inplace=True)

    # Per lo State fare una colonna per ogni valore
    #  xy_ab ---->   src con stati x, y   :   src con stati a, b
    #  src_x src_y src_q src_w dst_a dst_b dst_d dst_c 
    #     1     1     0     0     1     1     0     0

    listaStati = completeDataFrame['State'].to_list()
    listaDiListe = [i.split("_") for i in listaStati]
    valoriSrc = []
    valoriDst = []

    # ricerca di tutti i possibili stati per src e dst
    for lista in listaDiListe:
        for carattere in lista[0]:
            if carattere != ' ' and carattere not in valoriSrc:
                valoriSrc.append(carattere)
        
        for carattere in lista[1]:
            if carattere != ' ' and carattere not in valoriDst:
                valoriDst.append(carattere)

    # per ogni valore di src creo una colonna
    for val in valoriSrc:
        colonna = []
        for valDataset in listaDiListe:
            if val in valDataset[0]:
                colonna.append(1)
            else:
                colonna.append(0)
        completeDataFrame['StateSrc_' + val] = colonna
    
    # per ogni valore di dst creo una colonna
    for val in valoriDst:
        colonna = []
        for valDataset in listaDiListe:
            if val in valDataset[1]:
                colonna.append(1)
            else:
                colonna.append(0)
        completeDataFrame['StateDst_' + val] = colonna
    
    completeDataFrame.drop(columns='State', inplace=True)

    goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame = splitFromCompleteToSingle(completeDataFrame)
    scritturaSuFile(goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame)
    
    return goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame