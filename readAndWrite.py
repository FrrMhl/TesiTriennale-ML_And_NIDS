import os
import pandas as pd
import json





"""
    funzione per la creazione del dataframe completo partendo dai vari file csv
    parametri -> cartella in cui sono presenti i vari file
"""
def letturaCompleteDataFrameCTU13(netflowDirectory):

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
def splitFromCompleteToSingleCTU13(completeDataFrame):

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
def letturaSingleDataFrameCTU13():

    # apertura singoli dataframe
    with open('CTU13-Preprocessed/type.json', 'r') as f:
        type = json.load(f)

    goodDataFrame = pd.read_csv('CTU13-Preprocessed/goodNetFlow.csv', dtype=type, engine='pyarrow')
    nerisDataFrame = pd.read_csv('CTU13-Preprocessed/nerisNetFlow.csv', dtype=type, engine='pyarrow')
    rbotDataFrame = pd.read_csv('CTU13-Preprocessed/rbotNetFlow.csv', dtype=type, engine='pyarrow')
    virutDataFrame = pd.read_csv('CTU13-Preprocessed/virutNetFlow.csv', dtype=type, engine='pyarrow')
    mentiDataFrame = pd.read_csv('CTU13-Preprocessed/mentiNetFlow.csv', dtype=type, engine='pyarrow')
    murloDataFrame = pd.read_csv('CTU13-Preprocessed/murloNetFlow.csv', dtype=type, engine='pyarrow')

    return goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame





"""
    salavataggio dei dataset per ogni traffico su file csv
    parametri -> singoli dataset, uno per ogni traffico
"""
def scritturaSuFileCTU13(goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame):

    # salvataggio dataframe coi rispettivi dtype
    goodDataFrame.to_csv('CTU13-Preprocessed/goodNetFlow.csv', index=False)
    nerisDataFrame.to_csv('CTU13-Preprocessed/nerisNetFlow.csv', index=False)
    rbotDataFrame.to_csv('CTU13-Preprocessed/rbotNetFlow.csv', index=False)
    virutDataFrame.to_csv('CTU13-Preprocessed/virutNetFlow.csv', index=False)
    mentiDataFrame.to_csv('CTU13-Preprocessed/mentiNetFlow.csv', index=False)
    murloDataFrame.to_csv('CTU13-Preprocessed/murloNetFlow.csv', index=False)

    type = goodDataFrame.dtypes.to_frame('dtype')['dtype'].astype(str).to_dict()
    with open('CTU13-Preprocessed/type.json', 'w') as f:
        json.dump(type, f)





"""
    funzione per la creazione del dataframe completo partendo dai vari file csv
    parametri -> cartella in cui sono presenti i vari file
"""
def letturaCompleteDataFrameCICIDS(directory17, directory18):

    # creazione unico dataset
    singleDataFrame = []
    for filename in os.listdir(directory17): 
        df = pd.read_csv(os.path.join(directory17, filename))
        singleDataFrame.append(df)
    cicids17 = pd.concat(singleDataFrame, ignore_index=True)

    singleDataFrame = []
    for filename in os.listdir(directory18): 
        df = pd.read_csv(os.path.join(directory18, filename), engine='pyarrow')
        singleDataFrame.append(df)
    cicids18 = pd.concat(singleDataFrame, ignore_index=True)

    return cicids17, cicids18





"""
    funzione che divide cicids17 e 18 in benevolo e malevolo
    parametri -> due dataset cicids
"""
def splitIntoBenevoloMalevolo(cicids17, cicids18):

    colonne = cicids17.columns
    c17b = pd.DataFrame(columns=colonne)
    c17m = pd.DataFrame(columns=colonne)
    c18b = pd.DataFrame(columns=colonne)
    c18m = pd.DataFrame(columns=colonne)

    c17b = cicids17[cicids17['Label'] == 'BENIGN']
    c17b['Label'].replace({'BENIGN': 0}, inplace=True)
    c17b['Label'] = c17b['Label'].astype('uint8')

    c17m = cicids17[cicids17['Label'] == 'DoS Hulk']
    c17m['Label'].replace({'DoS Hulk': 1}, inplace=True)
    c17m['Label'] = c17m['Label'].astype('uint8')

    c18b = cicids18[cicids18['Label'] == 'Benign']
    c18b['Label'].replace({'Benign': 0}, inplace=True)
    c18b['Label'] = c18b['Label'].astype('uint8')

    c18m = cicids18[cicids18['Label'] == 'DoS attacks-Hulk']
    c18m['Label'].replace({'DoS attacks-Hulk': 1}, inplace=True)
    c18m['Label'] = c18m['Label'].astype('uint8')

    return c17b, c17m, c18b, c18m





"""
    funzione che salava benevoli e malevoli su file
    parametri -> dataset divisi in benevoli e malevoli
"""
def scritturaSuFileCICIDS(c17b, c17m, c18b, c18m):

    c17b.to_csv('CICIDS1718-Preprocessed/CICIDS17-benevoli.csv', index=False)
    c17m.to_csv('CICIDS1718-Preprocessed/CICIDS17-malevoli.csv', index=False)
    c18b.to_csv('CICIDS1718-Preprocessed/CICIDS18-benevoli.csv', index=False)
    c18m.to_csv('CICIDS1718-Preprocessed/CICIDS18-malevoli.csv', index=False)
    

    type = c17b.dtypes.to_frame('dtype')['dtype'].astype(str).to_dict()
    with open('CICIDS1718-Preprocessed/type.json', 'w') as f:
        json.dump(type, f)





"""
    funzione per l'apertura dei vari dataset benevoli e malevoli'
    parametri -> nessuno
"""
def letturaBenevoliMalevoliCICIDS():

    # apertura singoli dataframe
    with open('CICIDS1718-Preprocessed/type.json', 'r') as f:
        type = json.load(f)

    c17b = pd.read_csv('CICIDS1718-Preprocessed/CICIDS17-benevoli.csv', dtype=type, engine='pyarrow')
    c17m = pd.read_csv('CICIDS1718-Preprocessed/CICIDS17-malevoli.csv', dtype=type, engine='pyarrow')
    c18b = pd.read_csv('CICIDS1718-Preprocessed/CICIDS18-benevoli.csv', dtype=type, engine='pyarrow')
    c18m = pd.read_csv('CICIDS1718-Preprocessed/CICIDS18-malevoli.csv', dtype=type, engine='pyarrow')

    return c17b, c17m, c18b, c18m
