from .abstractConnector import AbstractConnector
from ..strings import NOT_WRITE_STRING,NOT_READ_STRING


class HiveConnector(AbstractConnector):
    def __init__(self,connectorType,StorageLevel=None,hiveDatabaseName=None,hiveTableName=None,hiveQuery=None,writeMode=None,*args,**kwargs):
        super().__init__(connectorType,StorageLevel)
        self.hiveDatabaseName = hiveDatabaseName
        self.hiveTableName = hiveTableName
        self.hiveQuery = hiveQuery
        self.writeMode = writeMode
    def readData(self,spark):
        if self.connectorType != self.SOURCE_CONNECTOR:
            raise Exception(NOT_READ_STRING)
        if self.hiveTableName is not None and self.hiveTableName != "":
            dataFrameInput = spark.read.table("{}.{}".format(self.hiveDatabaseName,self.hiveTableName))
        else:
            dataFrameInput = spark.sql(self.hiveQuery)
        return dataFrameInput
    def writeData(self,spark,dataInput):
        if self.connectorType != self.TARGET_CONNECTOR:
            raise Exception(NOT_WRITE_STRING)

        dataInput.write.mode(self.writeMode)\
            .saveAsTable("{}.{}".format(self.hiveDatabaseName, self.hiveTableName))


