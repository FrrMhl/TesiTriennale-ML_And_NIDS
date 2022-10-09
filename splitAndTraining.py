import pandas as pd
from sklearn.metrics import recall_score, precision_score, f1_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split





"""
    funzione per la suddivisione in tarining e testing set
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
    