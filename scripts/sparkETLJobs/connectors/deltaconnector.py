from delta.tables import *
from abstractConnector import AbstractConnector

class DeltaConnector(AbstractConnector):
    def __init__(self,
                 connectorType,
                 StorageLevel=None,
                 readWritePath=None,
                 deltaTableName=None,
                 writeMode=None,
                 mergeConditions=None,*args,**kwargs): ## write conditions deltatable.column = deltabale_SRC.columns
        super().__init__(connectorType,StorageLevel)
        self.readWritePath = readWritePath
        self.deltaTableName = deltaTableName
        self.writeMode = writeMode
        self.mergeConditions = mergeConditions
        self.dataFilePath = "{}/{}".format(self.readWritePath, self.deltaTableName)

    def readData(self, spark):
        dataFilePath = "{}/{}".format(self.readWritePath, self.deltaTableName)
        dataDF = spark.read.format("delta").load(dataFilePath)
        return dataDF

    def writeData(self, spark, dataInput):
        if self.writeMode == "append" or self.writeMode == "overwrite":
            dataInput.write.format("delta").mode(self.writeMode).save(self.dataFilePath)
        elif self.writeMode == "merge-delete":
            self.__mergeDelete__(spark,dataInput)
        elif self.writeMode == "merge-upsert":
            self.__mergeUpsert__(spark,dataInput)

    def __mergeDelete__(self,spark,dataInput):
        deltaTable = DeltaTable.forPath(spark, self.dataFilePath)
        deltaTable.alias(self.deltaTableName) \
            .merge(
            dataInput.alias(self.deltaTableName+"_SRC"),
            self.mergeConditions
        ).whenMatchedDelete().execute()

    def __mergeUpsert__(self,spark,dataInput):
        deltaTable = DeltaTable.forPath(spark, self.dataFilePath)
        deltaTable.alias(self.deltaTableName) \
            .merge(
            dataInput.alias(self.deltaTableName+"_SRC"),
            self.mergeConditions
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()







