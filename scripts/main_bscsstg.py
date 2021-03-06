from sparkETLJobs.baseetljob import ETLJob
from sparkETLJobs.connectors.hiveconnector import HiveConnector
from sparkETLJobs.connectors.hdfsconnector import HdfsConnector
import argparse

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
        """
        Class constructor
        :param jobName: Name of the job
        :param filePath: Full path for the input file
        :param fileName: the file name for the input file
        :param loadDate: load date used to to know which data folder to be used for loading. Format YYYY-MM-DD.
        :param hiveDatabaseName: Hive database name
        :param dataPath: full path for the data files.
        :param schemaPath: path for the schema files which will be used in loading the files.
        :param delimiter: The delimiter used in the input file.
        :param startLineNum: starting line number for the input file
        :param endLineNum: ending line number for the input file.
        """
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
        """
        This function do the whole logic for loading by reading teh required lines from the input file and kickoff the loading.
        :return: None
        """
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
                "connector":hdfsSource
            }]
        )
        etlJob.setLoadingList(
            [{
                "DataFrameName":hiveTableName,
                "connector":hiveTabTarget,
                "JobExecIdName":"FILE_ID"
            }]
        )
        etlJob.runETL()




if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='BSCS Batch Staging to Hive')
    parser.add_argument('jobName', metavar='jobName', type=str, nargs=1,
                        help='The name of the job')
    parser.add_argument('inputFilePath', metavar='inputFilePath', type=str, nargs=1,
                        help='Path for the input file')
    parser.add_argument('inputFileName', metavar="inputFileName", type=str, nargs=1,
                        help='Input file name')
    parser.add_argument('loadDate', metavar="loadDate", type=str, nargs=1,
                        help='Load date for the files in YYYY-MM-DD')
    parser.add_argument('hiveDatabase', metavar="hiveDatabase", type=str, nargs=1,
                        help='The staging database on Hive')
    parser.add_argument('dataFullPath', metavar="dataFullPath", type=str, nargs=1,
                        help='Full path for the data on HDFS')
    parser.add_argument('schemaFilesPath', metavar="schemaFilesPath", type=str, nargs=1,
                        help='Full path for the schema files')

    args = parser.parse_args()
    """
    args = parser.parse_args(["BSCS_1",
                              "/datalake/input_files",
                              "BSCSStagingFile.txt",
                              "2022-03-04",
                              "STAGING",
                              "/data/staging/BSCS",
                              "/datalake/schemas/BSCS"])
    """

    bscsJob = BSCSStgBatchJob(args.jobName[0],
                              args.inputFilePath[0],
                              args.inputFileName[0],
                              args.loadDate[0],
                              args.hiveDatabase[0],
                              args.dataFullPath[0],
                              args.schemaFilesPath[0])

    bscsJob.startLoading()
