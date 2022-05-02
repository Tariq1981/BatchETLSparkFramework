class EtlJobEngine:
    def __init__(self,spark,sourceConnector):
        self.spark = spark
        self.sourceConnector = sourceConnector
    def extractData(self):
        return self.sourceConnector.readData(self.spark)
    def trasformData(self,funcList,dfData):
        if len(funcList) == 0:
            return dfData
        return self.trasformData(funcList[1:],dfData.transform(funcList[0]))

    def trasformListData(self,funcListOfList,dfDataList):
        ls = []
        for i in range(0,len(dfDataList)):
             ls.append(self.trasformData(funcListOfList[i],dfDataList[i]))
        return ls

    #def getListofTransformations(self):
    #    return [[]]
    ### Pass it from the caller
    def loadData(self,dataFrameList,targetConnectorsList):
        print("DataFrames to be loaded {}".format(len(dataFrameList)))
        if len(dataFrameList) == 0:
            return
        targetConnectorsList[0].writeData(self.spark,dataFrameList[0])
        self.loadData(dataFrameList[1:],targetConnectorsList[1:])





