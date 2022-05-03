import json
from pyspark.sql.types import StructType
from .abstractConnector import AbstractConnector
from ..strings import NOT_READ_STRING,NOT_WRITE_STRING


class HdfsConnector(AbstractConnector):

    def __init__(self,
                 connectorType,
                 StorageLevel=None,
                 readWritePath=None,
                 fileNamePattern=None,
                 schemaFilePath=None,
                 schemaFileName=None,
                 fileFormat=None,
                 isHeader=None,
                 dateFormat=None,
                 timeStampFormat=None,
                 writeMode=None,*args,**kwargs):
        super().__init__(connectorType,StorageLevel)
        self.readWritePath = readWritePath
        self.fileNamePattern = fileNamePattern
        self.schemaFilePath = schemaFilePath
        self.schemaFileName = schemaFileName
        self.fileFormat = fileFormat
        self.isHeader = isHeader
        self.dateFormat = dateFormat
        self.timeStampFormat = timeStampFormat
        self.writeMode = writeMode

    def getStagingSchema(self):
        schemaPath = "{}/{}".format(self.schemaFilePath,self.schemaFileName)
        with open(schemaPath,"r") as file:
            d = json.load(file)
            schema = StructType.fromJson(d)
        return schema

    def readData(self,spark):
        if self.connectorType != self.SOURCE_CONNECTOR:
            raise Exception(NOT_READ_STRING)
        dataFilePath = "{}/{}".format(self.readWritePath, self.fileNamePattern)
        if self.fileFormat == "parquet":
            dataDF = spark.read.format(self.fileFormat).load(dataFilePath)
        else:
            schema = self.getStagingSchema()
            dataDF = spark.read.format(self.fileFormat).schema(schema)\
                .option("header", self.isHeader)\
                .option("inferSchema", "false") \
                .option("timestampFormat", self.timeStampFormat) \
                .option("dateFormat", self.dateFormat) \
                .load(dataFilePath)

        return dataDF

    def writeData(self,spark,dataInput):
        if self.connectorType != self.TARGET_CONNECTOR:
            raise Exception(NOT_WRITE_STRING)
        dataInput.write.mode(self.writeMode)\
            .format(self.fileFormat).save(self.readWritePath)
