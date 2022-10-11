from readAndWrite import letturaBenevoliMalevoliCICIDS, letturaCompleteDataFrameCTU13, letturaSingleDataFrameCTU13, letturaCompleteDataFrameCICIDS
from preprocessing import preprocessingCTU13, preprocessingCICIDS
from splitAndTraining import splitForTrainingCICIDS, splitForTrainingCTU13, trainCTU13, trainCICIDS
import os
from evaluation import ensambleCICIDS, reduceRecallCICIDS, reduceRecallCTU13, scambioMalevoliCICIDS
import numpy as np

if __name__ == '__main__':
    
    # codice per CTU-13 
    if not os.path.exists('CTU13-Preprocessed/goodNetFlow.csv'):

        netflowDirectory = os.path.join(os.getcwd(), 'NetFlowCTU13')
        completeDataFrame = letturaCompleteDataFrameCTU13(netflowDirectory)
        goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame = preprocessingCTU13(completeDataFrame)

    else:
        goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame = letturaSingleDataFrameCTU13()

    dataset = splitForTrainingCTU13(goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame)
    modelloRbotAddestrato = trainCTU13(dataset)

    reduceRecallCTU13(rbotDataFrame, modelloRbotAddestrato)


    # codice per CICIDS2017 e CICIDS2018  
    if not os.path.exists('CICIDS1718-Preprocessed/CICIDS17-benevoli.csv'):

        directory17 = os.path.join(os.getcwd(), 'NetFlowCICIDS2017')
        directory18 = os.path.join(os.getcwd(), 'NetFlowCICIDS2018')
        cicids2017, cicids2018 = letturaCompleteDataFrameCICIDS(directory17, directory18)
        c17b, c17m, c18b, c18m = preprocessingCICIDS(cicids2017, cicids2018)

    else:
        c17b, c17m, c18b, c18m = letturaBenevoliMalevoliCICIDS()
        
    combinazioni, nomi, ensamble = splitForTrainingCICIDS(c17b, c17m, c18b, c18m)
    modello17, modello18, bestModel = trainCICIDS(combinazioni, nomi, ensamble)
    
    scambioMalevoliCICIDS(modello17, c18m, modello18, c17m)
    attacchi17, attacchi18 = reduceRecallCICIDS(modello17, c17m, modello18, c18m)
    ensambleCICIDS(bestModel, attacchi17, attacchi18)
