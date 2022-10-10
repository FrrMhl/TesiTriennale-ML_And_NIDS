import pandas as pd
from sklearn.metrics import recall_score, precision_score, f1_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split





"""
    funzione per la suddivisione in training e testing set
    parametri -> vari dataset da suddividere
"""
def splitForTrainingCTU13(goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame):

    # creazione train e test set (proporzione 1:20)
    dataset = []
    df = [nerisDataFrame, goodDataFrame.sample(len(nerisDataFrame) * 20)]
    dataset.append(pd.concat(df, ignore_index=True))
    df = [rbotDataFrame, goodDataFrame.sample(len(rbotDataFrame) * 20)]
    dataset.append(pd.concat(df, ignore_index=True))
    df = [virutDataFrame, goodDataFrame.sample(len(virutDataFrame) * 20)]
    dataset.append(pd.concat(df, ignore_index=True))
    df = [mentiDataFrame, goodDataFrame.sample(len(mentiDataFrame) * 20)]
    dataset.append(pd.concat(df, ignore_index=True))
    df = [murloDataFrame, goodDataFrame.sample(len(murloDataFrame) * 20)]
    dataset.append(pd.concat(df, ignore_index=True))

    return dataset





"""
    funzione per l'addestramento e la valutazione delle performance
    parametri -> lista con i vari dataset
"""
def trainCTU13(dataset):

    nomi = ['Neris', 'Rbot', 'Virut', 'Menti', 'Murlo']
    for (nome, df) in zip(nomi, dataset):

        # addestramento e valutazione
        X = df.drop('Label', axis=1)
        y = df['Label']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2) 

        clf = RandomForestClassifier()
        clf.fit(X_train, y_train)
        if nome == 'Rbot':
            modelloRbotAddestrato = clf
        val = clf.predict(X_test)

        print('\n\n\n\n\n-----------------------------------------------------------------')
        print('Modello con -> {}'.format(nome))
        print('Recall is {}'.format(recall_score(y_test, val)))
        print('Precision is {}'.format(precision_score(y_test, val)))
        print('F1-Score is {}'.format(f1_score(y_test, val)))

    return modelloRbotAddestrato       





"""
    funzione per la creazione delle varie combinazioni tra i vari dataset
    parametri -> vari dataset da combinare tra loro
"""
def splitForTrainingCICIDS(c17b, c17m, c18b, c18m):

    combinazioni = []
    nomi = []

    # benevoli 17 malevoli 17
    nomi.append(['Benevoli CICIDS_2017', 'Malevoli CICIDS_2017'])
    df = pd.concat([c17b, c17m], ignore_index=True)
    combinazioni.append(df)

    # benevoli 17 malevoli 18
    nomi.append(['Benevoli CICIDS_2017', 'Malevoli CICIDS_2018'])
    df = pd.concat([c17b, c18m], ignore_index=True)
    combinazioni.append(df)

    # benevoli 18 malevoli 17
    nomi.append(['Benevoli CICIDS_2018', 'Malevoli CICIDS_2017'])
    df = pd.concat([c18b, c17m], ignore_index=True)
    combinazioni.append(df)

    # benevoli 18 malevoli 18
    nomi.append(['Benevoli CICIDS_2018', 'Malevoli CICIDS_2018'])
    df = pd.concat([c18b, c18m], ignore_index=True)
    combinazioni.append(df)

    return combinazioni, nomi





"""
    funzione per l'addestramento e la valutazione delle performance delle combinazioni di CICIDS
    parametri -> lista con le varie combianazioni e nomi
"""
def trainCICIDS(combinazioni, nomi):

    # per prendeere i modelli addestrati 0 e 3 che sono i modelli completi cicids17 e cicids18
    i = 0
    modelliAddestrati = []
    for (combinazione, nome) in zip(combinazioni, nomi):

        # addestramento e valutazione
        X = combinazione.drop('Label', axis=1)
        y = combinazione['Label']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2) 

        clf = RandomForestClassifier()
        clf.fit(X_train, y_train)
        # prendo il modello di cicids17 e cicids18 e li provo con i malevoli invertiti
        if i == 0 or i == 3:
            modelliAddestrati.append(clf)
        val = clf.predict(X_test)

        print('\n\n\n\n\n-----------------------------------------------------------------')
        print('Modello con -> {} e {}'.format(nome[0], nome[1]))
        print('Recall is {}'.format(recall_score(y_test, val)))
        print('Precision is {}'.format(precision_score(y_test, val)))
        print('F1-Score is {}'.format(f1_score(y_test, val)))

        i += 1

    return modelliAddestrati
