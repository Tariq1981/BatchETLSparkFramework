from baseetljob import ETLJob
from connectors.hiveconnector import HiveConnector
from connectors.hdfsconnector import HdfsConnector
from etlutils import EtlUtils
from pyspark.sql.functions import lit

class BSCSStgBatchJob:
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

        job = ETLJob(self.jobName)
        fullPath = "{}/{}".format(self.filePath,self.fileName)
        with open(fullPath,"r") as file:
            print("File Openned !!")
            for lineNum,line in enumerate(file):
                if (lineNum+1) > self.endLineNum:
                    break
                if (lineNum+1) >= self.startLineNum:
                    lineList = line.split("|")
                    self.loadBSCSFileIntoHive(job,lineList[1],lineList[2],lineList[3])
        print("End !!!!!")
    def loadBSCSFileIntoHive(self,etlJob,hiveTableName,filePatternname,schemaFileName):
        dataFullPath = "{}/{}".format(self.dataPath, self.loadDate)
        hiveTabTarget = HiveConnector(HiveConnector.TARGET_CONNECTOR,
                                      None,
                                      self.hiveDatabaseName,
                                      hiveTableName,
                                      writeMode="overwrite")
        hdfsSource = HdfsConnector(HdfsConnector.SOURCE_CONNECTOR,
                                   None,
                                   dataFullPath,
                                   filePatternname,
                                   self.schemaPath,
                                   schemaFileName,
                                   BSCSStgBatchJob.fileFormat,
                                   BSCSStgBatchJob.isHeader,
                                   BSCSStgBatchJob.dateFormat,
                                   BSCSStgBatchJob.timeStampFormat
                                   )
        etlJob.setExtractingSourcesList(
            [{
                "DataFrameName":hiveTableName,
                "Source":hdfsSource
            }]
        )
        etlJob.setLoadingList(
            [{
                "DataFrameName":hiveTableName,
                "Target":hiveTabTarget,
                "JobExecIdName":"FILE_ID"
            }]
        )
        etlJob.runETL()

    def addFileId(self,dfInput,*args,**kwargs):
        fileId=EtlUtils.getUniqueID()
        return dfInput.withColumn("FILE_ID", lit(fileId))

