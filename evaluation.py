from sklearn.metrics import recall_score





"""
    funzione per la modifica di alcune feature per evadere la NIDS
    parametri -> dataset contenente il traffico malevolo e il modello addestrato in precedenza
"""
def reduceRecall(dataSetMalevolo, modelloAddestrato):

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
    print('Modelli con campi modificati\n')
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
