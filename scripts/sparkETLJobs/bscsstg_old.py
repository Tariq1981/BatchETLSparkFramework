from etljobengine import EtlJobEngine
from connectors.hiveconnector import HiveConnector
from connectors.hdfsconnector import HdfsConnector
from etlutils import EtlUtils
from pyspark.sql.functions import lit

class BSCSStgJob:
    writeMode = "overwrite"
    fileFormat = "csv"
    isHeader = "true"
    inferSchema = "false"
    dateFormat = "MM/dd/yyyy"
    timeStampFormat = "MM/dd/yyyy HH:mm:ss"
    def __init__(self,
                 jobName,
                 filePath,
                 fileName,
                 loadDate,
                 hiveDatabaseName,
                 dataPath,
                 schemaPath,
                 delimiter="|",
                 startLineNum=1,
                 endLineNum=1
                 ):
        self.jobName = jobName
        self.spark = EtlUtils.getSparkSession(jobName)
        self.filePath = filePath
        self.fileName = fileName
        self.loadDate = loadDate
        self.delimiter = delimiter
        self.startLineNum = startLineNum
        self.endLineNum = endLineNum
        self.hiveDatabaseName = hiveDatabaseName
        self.dataPath = dataPath
        self.schemaPath = schemaPath



    def startLoading(self):
        if self.endLineNum < self.startLineNum:
            return

        fullPath = "{}/{}".format(self.filePath,self.fileName)
        with open(fullPath,"r") as file:
            print("File Openned !!")
            for lineNum,line in enumerate(file):
                if (lineNum+1) > self.endLineNum:
                    break
                if (lineNum+1) >= self.startLineNum:
                    lineList = line.split("|")
                    self.loadBSCSFileIntoHive(lineList[1],lineList[2],lineList[3])
        print("End !!!!!")
    def loadBSCSFileIntoHive(self,hiveTableName,filePatternname,schemaFileName):
        dataFullPath = "{}/{}".format(self.dataPath, self.loadDate)
        hiveTabTarget = HiveConnector(HiveConnector.TARGET_CONNECTOR,
                                      self.hiveDatabaseName,
                                      hiveTableName,
                                      writeMode="overwrite")
        hdfsSource = HdfsConnector(HdfsConnector.SOURCE_CONNECTOR,
                                   dataFullPath,
                                   filePatternname,
                                   self.schemaPath,
                                   schemaFileName,
                                   BSCSStgJob.fileFormat,
                                   BSCSStgJob.isHeader,
                                   BSCSStgJob.dateFormat,
                                   BSCSStgJob.timeStampFormat
                                   )
        etlJob = EtlJobEngine(self.spark, hdfsSource)
        dfInput = etlJob.extractData()
        dfList = etlJob.trasformListData([[self.addFileId]],[dfInput])
        etlJob.loadData(dfList,[hiveTabTarget])
    def addFileId(self,dfInput,*args,**kwargs):
        fileId=EtlUtils.getUniqueID()
        return dfInput.withColumn("FILE_ID", lit(fileId))

