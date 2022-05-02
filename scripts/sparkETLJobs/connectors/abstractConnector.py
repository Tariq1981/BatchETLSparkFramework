
class AbstractConnector:
    SOURCE_CONNECTOR = 1
    TARGET_CONNECTOR = 2
    def __init__(self,connectorType,StorageLevel=None,*args,**kwargs):
        self.connectorType = connectorType
        self.StorageLevel = StorageLevel
    def readData(self,spark):
        pass
    def writeData(self,spark,dataInput):
        pass




