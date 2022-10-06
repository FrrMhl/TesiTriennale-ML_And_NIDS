import os
import pandas as pd
import json





"""
    funzione per la creazione del dataframe completo partendo dai vari file csv
    parametri -> cartella in cui sono presenti i vari file
"""
def letturaCompleteDataFrame(netflowDirectory):

    # creazione unico dataset modificando la label
    singleDataFrame = []

    for filename in os.listdir(netflowDirectory): 

        df = pd.read_csv(os.path.join(netflowDirectory, filename), engine='pyarrow')
        label = df['Label'].to_list()
        nuoviValori = [
            'Neris' if 'Botnet-V42' in i or 'Botnet-V43' in i or 'Botnet-V50' in i else 
            'Rbot' if 'Botnet-V44' in i or 'Botnet-V45' in i or 'Botnet-V51' in i or 'Botnet-V52' in i else 
            'Virut' if 'Botnet-V46' in i or 'Botnet-V54' in i else 
            'Menti' if 'Botnet-V47' in i else 
            'Murlo' if 'Botnet-V49' in i else
            0
            for i in label]
        nuoviValori = pd.Series(nuoviValori)
        df.drop(columns='Label', inplace=True)
        df['Label'] = nuoviValori

        singleDataFrame.append(df)

    completeDataFrame = pd.concat(singleDataFrame, ignore_index=True)

    return completeDataFrame




"""
    divisione del dataset completo in uno per ogni traffico
    parametri -> dataset completo
"""
def splitFromCompleteToSingle(completeDataFrame):

    # suddivisione in dataset per ogni botnet e traffico benevolo
    colonne = completeDataFrame.columns
    goodDataFrame = pd.DataFrame(columns=colonne)
    nerisDataFrame = pd.DataFrame(columns=colonne)
    rbotDataFrame = pd.DataFrame(columns=colonne)
    virutDataFrame = pd.DataFrame(columns=colonne)
    mentiDataFrame = pd.DataFrame(columns=colonne)
    murloDataFrame = pd.DataFrame(columns=colonne)

    goodDataFrame = completeDataFrame[completeDataFrame['Label'] == 0]
    goodDataFrame['Label'] = goodDataFrame['Label'].astype('uint8')

    nerisDataFrame = completeDataFrame[completeDataFrame['Label'] == 'Neris']
    nerisDataFrame['Label'].replace({'Neris': 1}, inplace=True)
    nerisDataFrame['Label'] = nerisDataFrame['Label'].astype('uint8')

    rbotDataFrame = completeDataFrame[completeDataFrame['Label'] == 'Rbot']
    rbotDataFrame['Label'].replace({'Rbot': 1}, inplace=True)
    rbotDataFrame['Label'] = rbotDataFrame['Label'].astype('uint8')

    virutDataFrame = completeDataFrame[completeDataFrame['Label'] == 'Virut']
    virutDataFrame['Label'].replace({'Virut': 1}, inplace=True)
    virutDataFrame['Label'] = virutDataFrame['Label'].astype('uint8')

    mentiDataFrame = completeDataFrame[completeDataFrame['Label'] == 'Menti']
    mentiDataFrame['Label'].replace({'Menti': 1}, inplace=True)
    mentiDataFrame['Label'] = mentiDataFrame['Label'].astype('uint8')

    murloDataFrame = completeDataFrame[completeDataFrame['Label'] == 'Murlo']
    murloDataFrame['Label'].replace({'Murlo': 1}, inplace=True, )
    murloDataFrame['Label'] = murloDataFrame['Label'].astype('uint8')

    return goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame





"""
    funzione per l'apertura dei vari dataset, ognuno per ogni traffico'
    parametri -> nessuno
"""
def letturaSingleDataFrame():

    # apertura singoli dataframe
    with open('type.json', 'r') as f:
        type = json.load(f)

    goodDataFrame = pd.read_csv('goodNetFlow.csv', dtype=type, engine='pyarrow')
    nerisDataFrame = pd.read_csv('nerisNetFlow.csv', dtype=type, engine='pyarrow')
    rbotDataFrame = pd.read_csv('rbotNetFlow.csv', dtype=type, engine='pyarrow')
    virutDataFrame = pd.read_csv('virutNetFlow.csv', dtype=type, engine='pyarrow')
    mentiDataFrame = pd.read_csv('mentiNetFlow.csv', dtype=type, engine='pyarrow')
    murloDataFrame = pd.read_csv('murloNetFlow.csv', dtype=type, engine='pyarrow')

    return goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame





"""
    salavataggio dei dataset per ogni traffico su file csv
    parametri -> singoli dataset, uno per ogni traffico
"""
def scritturaSuFile(goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame):

    # salvataggio dataframe coi rispettivi dtype
    goodDataFrame.to_csv('goodNetFlow.csv', index=False)
    nerisDataFrame.to_csv('nerisNetFlow.csv', index=False)
    rbotDataFrame.to_csv('rbotNetFlow.csv', index=False)
    virutDataFrame.to_csv('virutNetFlow.csv', index=False)
    mentiDataFrame.to_csv('mentiNetFlow.csv', index=False)
    murloDataFrame.to_csv('murloNetFlow.csv', index=False)

    type = goodDataFrame.dtypes.to_frame('dtype')['dtype'].astype(str).to_dict()
    with open('type.json', 'w') as f:
        json.dump(type, f)
