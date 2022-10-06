from readAndWrite import letturaCompleteDataFrame, letturaSingleDataFrame
from preprocessing import preprocessing
from splitAndTraining import splitForTraining, train
import os
from evaluation import reduceRecall


if __name__ == '__main__':
    
    if not os.path.exists('goodNetFlow.csv'):

        netflowDirectory = os.path.join(os.getcwd(), 'NetFlowCTU13')
        completeDataFrame = letturaCompleteDataFrame(netflowDirectory)
        goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame = preprocessing(completeDataFrame)

    else:
        goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame = letturaSingleDataFrame()

    dataset = splitForTraining(goodDataFrame, nerisDataFrame, rbotDataFrame, virutDataFrame, mentiDataFrame, murloDataFrame)
    modelloRbotAddestrato = train(dataset)

    reduceRecall(rbotDataFrame, modelloRbotAddestrato)
                
    #  trovare attacchi comuni con altri dataset
