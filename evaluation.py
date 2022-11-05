from sklearn.metrics import recall_score
import numpy as np
import pandas as pd

pd.options.mode.use_inf_as_na = True





"""
    funzione per la modifica di alcune feature per evadere la NIDS
    parametri -> dataset contenente il traffico malevolo e il modello addestrato in precedenza
"""
def reduceRecallCTU13(dataSetMalevolo, modelloAddestrato):

    featureToChange = {
        'Duration': 0,
        'OutBytes': 1,
        'InBytes': 2,
        'TotPkts': 3
    }

    incrementToApply = {
        '1': [1, 1, 1, 1],
        '2': [2, 2, 2, 2],
        '3': [5, 8, 8, 5],
        '4': [10, 16, 16, 10],
        '5': [15, 64, 64, 15],
        '6': [30, 128, 128, 20],
        '7': [45, 256, 256, 30],
        '8': [60, 512, 512, 50],
        '9': [120, 1024, 1024, 100]
    }

    groupOfFeatures = {
        '1a': ['Duration'],
        '1b': ['OutBytes'],
        '1c': ['InBytes'],
        '1d': ['TotPkts'],
        '2a': ['Duration', 'OutBytes'],
        '2b': ['Duration', 'InBytes'],
        '2c': ['Duration', 'TotPkts'],
        '2d': ['OutBytes', 'InBytes'],
        '2e': ['OutBytes', 'TotPkts'],
        '2f': ['InBytes', 'TotPkts'],
        '3a': ['Duration', 'OutBytes', 'InBytes'],
        '3b': ['Duration', 'OutBytes', 'TotPkts'],
        '3c': ['Duration', 'InBytes', 'TotPkts'],
        '3d': ['OutBytes', 'InBytes','TotPkts'],
        '4a': ['Duration', 'OutBytes', 'InBytes', 'TotPkts'],
    }

    # ogni gruppo (15) lo faccio passare per ogni incremento (9)
    # in totale avrò 15 * 9 = 135 dataset modificati
    # da cambiare oltre alle 4 feature:
    # totbytes = out + in
    # bytesperpkts = totbytes/totpkts
    # pktspersec = totpkts/duration
    # ratiooutin = out/in

    print('\n\n\n\n\n-----------------------------------------------------------------')
    print('Modello Rbot con campi modificati\n')
    indice = 1
    for gruppo, listaFeature in groupOfFeatures.items():
        for key, incrementi in incrementToApply.items():

            df = dataSetMalevolo.copy()
            for feature in listaFeature:
                indexOfIncrement = featureToChange.get(feature)
                df[feature] += incrementi[indexOfIncrement]

            # modifico le feature derivate
            df['RatioOutIn'] = df['OutBytes'] / df['InBytes']
            df['TotBytes'] = df['OutBytes'] + df['InBytes']
            df['PktsPerSec'] = df['TotPkts'] / df['Duration']
            df['BytesPerPkt'] = df['TotBytes'] / df['TotPkts']

            # valuto questo nuovo modello
            val = modelloAddestrato.predict(df.drop(columns='Label'))
            print('{} -> Recall for group {} and increment {} is {}'.format(indice, gruppo, key, recall_score(df['Label'], val)))
            indice += 1
        print('\n')





"""
    funzione che prende il modello cicids17(cicids18) e lo prova coi malevoli di cicids18(cicids17)
    parametri -> modelli addestrati e malevoli
"""
def scambioMalevoliCICIDS(modello17, c18m, modello18, c17m):

    val = modello17.predict(c18m.drop(columns='Label'))
    print('\n\n\n\n\n-----------------------------------------------------------------')
    print('Modello completo CICIDS_2017 con malevoli di CICIDS_2018')
    print('Recall is -> {}'.format(recall_score(c18m['Label'], val)))

    val = modello18.predict(c17m.drop(columns='Label'))
    print('\n\n\n\n\n-----------------------------------------------------------------')
    print('Modello completo CICIDS_2018 con malevoli di CICIDS_2017')
    print('Recall is -> {}'.format(recall_score(c17m['Label'], val)))





"""
    funzione per la modifica di alcune feature per evadere la NIDS di CICIDS2017 e CICIDS2018
    parametri -> dataset contenente il traffico malevolo e il modello addestrato in precedenza
"""
def reduceRecallCICIDS(modello17, c17m, modello18, c18m):

    featureToChange = {
        'Flow Duration': 0,
        'TotLen Fwd Pkts': 1,
        'TotLen Bwd Pkts': 2,
        'Tot Pkts': 3
    }

    incrementToApply = {
        '1': [1, 1, 1, 1],
        '2': [2, 2, 2, 2],
        '3': [5, 8, 8, 5],
        '4': [10, 16, 16, 10],
        '5': [15, 64, 64, 15],
        '6': [30, 128, 128, 20],
        '7': [45, 256, 256, 30],
        '8': [60, 512, 512, 50],
        '9': [120, 1024, 1024, 100]
    }

    groupOfFeatures = {
        '1a': ['Flow Duration'],
        '1b': ['TotLen Fwd Pkts'],
        '1c': ['TotLen Bwd Pkts'],
        '1d': ['Tot Pkts'],
        '2a': ['Flow Duration', 'TotLen Fwd Pkts'],
        '2b': ['Flow Duration', 'TotLen Bwd Pkts'],
        '2c': ['Flow Duration', 'Tot Pkts'],
        '2d': ['TotLen Fwd Pkts', 'TotLen Bwd Pkts'],
        '2e': ['TotLen Fwd Pkts', 'Tot Pkts'],
        '2f': ['TotLen Bwd Pkts', 'Tot Pkts'],
        '3a': ['Flow Duration', 'TotLen Fwd Pkts', 'TotLen Bwd Pkts'],
        '3b': ['Flow Duration', 'TotLen Fwd Pkts', 'Tot Pkts'],
        '3c': ['Flow Duration', 'TotLen Bwd Pkts', 'Tot Pkts'],
        '3d': ['TotLen Fwd Pkts', 'TotLen Bwd Pkts','Tot Pkts'],
        '4a': ['Flow Duration', 'TotLen Fwd Pkts', 'TotLen Bwd Pkts', 'Tot Pkts'],
    }

    modelli = [modello17, modello18]
    malevoli = [c18m, c17m]
    attacchiCICIDS2017 = []
    attacchiCICIDS2018 = []

    print('\n\n\n\n\n-----------------------------------------------------------------')
    print('Modello CICIDS_2017 con campi modificati')
    for (modello, malevolo) in zip(modelli, malevoli):
        indice = 1

        for gruppo, listaFeature in groupOfFeatures.items():
            for key, incrementi in incrementToApply.items():

                df = malevolo.copy()
                
                for feature in listaFeature:
                    indexOfIncrement = featureToChange.get(feature)
                    df[feature] += incrementi[indexOfIncrement]

                # modifico le feature derivate (se ci sono NaN li sostiruisco con il valore max della colonna)
                df['TotLen Pkts'] = df['TotLen Fwd Pkts'] + df['TotLen Bwd Pkts']

                df['TotLen Per Pkts'] = df['TotLen Pkts'] / df['Tot Pkts']
                df['TotLen Per Pkts'].replace(np.nan, df['TotLen Per Pkts'].max(), inplace=True)

                df['Down/Up Ratio'] =  df['TotLen Bwd Pkts'] / df['TotLen Fwd Pkts']
                df['Down/Up Ratio'].replace(np.nan, df['Down/Up Ratio'].max(), inplace=True)

                df['Flow Pkts/s'] = df['Tot Pkts'] / df['Flow Duration']
                df['Flow Pkts/s'].replace(np.nan, df['Flow Pkts/s'].max(), inplace=True)

                # salvo gli attacchi da valutare in seguito sugli ensamble
                if modelli.index(modello) == 0:
                    attacchiCICIDS2017.append(df)
                else:
                    attacchiCICIDS2018.append(df)

                # valuto questo nuovo modello
                val = modello.predict(df.drop(columns='Label'))
                print('{} -> Recall for group {} and increment {} is {}'.format(indice, gruppo, key, recall_score(df['Label'], val)))
                indice += 1
            print('\n')

        if modelli.index(modello) == 0:
            print('\n\n\n\n\n-----------------------------------------------------------------')
            print('Modello CICIDS_2018 con campi modificati')

    return attacchiCICIDS2017, attacchiCICIDS2018





"""
    funzione per lprovare gli ensamble creati sugli attacchi dovuti alla modifica delle features
    parametri -> ensamble e attacchi
"""
def ensambleCICIDS(bestModel, attacchi17, attacchi18):

    print('\n\n\n\n\n-----------------------------------------------------------------')
    print('Modello addestrato su benevoli e malevoli di CICIDS_2017 + malevoli CICIDS_2018 valutato su attacchi di CICIDS_2018')
    indice = 1
    for attacco in attacchi17:
        val = bestModel[0].predict(attacco.drop(columns='Label'))
        print('{} -> Recall is {}'.format(indice, recall_score(attacco['Label'], val)))
        indice += 1


    print('\n\n\n\n\n-----------------------------------------------------------------')
    print('Modello addestrato su benevoli e malevoli di CICIDS_2018 + malevoli CICIDS_2017 valutato su attacchi di CICIDS_2017')
    indice = 1
    for attacco in attacchi18:
        val = bestModel[1].predict(attacco.drop(columns='Label'))
        print('{} -> Recall is {}'.format(indice, recall_score(attacco['Label'], val)))
        indice += 1
