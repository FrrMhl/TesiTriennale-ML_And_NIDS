from sklearn.metrics import recall_score
import numpy as np





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
            df['BytesPerPkt'] = df['TotPkts'] / df['TotBytes']

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
    print('Modello completo CICIDS2017 con malevoli di CICIDS2018')
    print('Recall is -> {}'.format(recall_score(c18m['Label'], val)))

    val = modello18.predict(c17m.drop(columns='Label'))
    print('\n\n\n\n\n-----------------------------------------------------------------')
    print('Modello completo CICIDS2018 con malevoli di CICIDS2017')
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
    malevoli = [c17m, c18m]

    print('\n\n\n\n\n-----------------------------------------------------------------')
    print('Modello CICIDS_2017 con campi modificati\n')
    for (modello, malevolo) in zip(modelli, malevoli):
        indice = 1

        for gruppo, listaFeature in groupOfFeatures.items():
            for key, incrementi in incrementToApply.items():

                df = malevolo.copy()
                
                for feature in listaFeature:
                    indexOfIncrement = featureToChange.get(feature)
                    df[feature] += incrementi[indexOfIncrement]

                # modifico le feature derivate (sostituisco gli 0 con il valore minimo della colonna per non avere NaN)
                df['TotLen Bwd Pkts'].replace(0, np.nan, inplace=True)
                df['TotLen Bwd Pkts'].replace(np.nan, df['TotLen Bwd Pkts'].min(), inplace=True)

                df['Flow Duration'].replace(0, np.nan, inplace=True)
                df['Flow Duration'].replace(np.nan, df['Flow Duration'].min(), inplace=True)

                df['Down/Up Ratio'] = df['TotLen Fwd Pkts'] / df['TotLen Bwd Pkts']
                df['Flow Pkts/s'] = df['Tot Pkts'] / df['Flow Duration']

                # valuto questo nuovo modello
                val = modello.predict(df.drop(columns='Label'))
                print('{} -> Recall for group {} and increment {} is {}'.format(indice, gruppo, key, recall_score(df['Label'], val)))
                indice += 1
            print('\n')
    
        print('\n\n\n\n\n-----------------------------------------------------------------')
        print('Modello CICIDS_2018 con campi modificati\n')
