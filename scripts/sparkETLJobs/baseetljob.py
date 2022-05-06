from .etlutils import EtlUtils
from .connectors.abstractConnector import AbstractConnector
from pyspark import StorageLevel
from pyspark.sql.functions import lit

class ETLJob:
    storageMap={
        "DISK_ONLY":StorageLevel.DISK_ONLY,
        "DISK_ONLY_2":StorageLevel.DISK_ONLY_2,
        "DISK_ONLY_3":StorageLevel.DISK_ONLY_3,
        "MEMORY_ONLY":StorageLevel.MEMORY_ONLY,
        "MEMORY_ONLY_2":StorageLevel.MEMORY_ONLY_2,
        "MEMORY_AND_DISK":StorageLevel.MEMORY_AND_DISK,
        "MEMORY_AND_DISK_2":StorageLevel.MEMORY_AND_DISK_2,
        "OFF_HEAP":StorageLevel.OFF_HEAP,
        "MEMORY_AND_DISK_DESER":StorageLevel.MEMORY_AND_DISK_DESER,
        "DEFAULT":StorageLevel.MEMORY_AND_DISK_DESER
    }
    def __init__(self,jobName,sourceConnectors=None,transList=None,dfConnectorsList=None,*args,**kwargs):
        self.jobName = jobName
        self.spark = EtlUtils.getSparkSession(jobName)
        self.sourceConnectors = sourceConnectors
        self.transList = transList
        self.dfConnectorsList = dfConnectorsList
    def setExtractingSourcesList(self,connectors):
        self.sourceConnectors = connectors
    def setTrasnformationsList(self,transList):
        """

        :param transFunList: pair of tranformation function and name
        :return:
        """
        self.transList = transList
    def setLoadingList(self,dfConnectorsList):
        self.dfConnectorsList = dfConnectorsList
    def runETL(self):
        tables = self.__readSources__()
        tablesTrans = self.__transformData__(self.transList,tables)
        self.__loadDataFrame__(tablesTrans)

    def __readSources__(self):
        tables={}
        for source in self.sourceConnectors:
            connectName = source["DataFrameName"]
            connector = source["connector"]
            tables[connectName] = connector.readData(self.spark)
            self.__presistDataFrame__(tables[connectName], connector.StorageLevel)
            tables[connectName].createOrReplaceTempView(connectName)
        return tables

    def __transformData__(self,transFunList,tables):
        """

        :param transFunList: List of (Map{"Name":"name of dataframe","Function":funciton object,"StorageLevel": storage level})
        :param tables: list of tables
        :return: list of tables trasformed
        """
        if not transFunList or len(transFunList) == 0:
            return tables
        transMap = transFunList[0]
        tables[transMap["Name"]] = transMap["Function"](self.spark,tables)
        self.__presistDataFrame__(tables[transMap["Name"]],transMap.get("StorageLevel",None))
        return self.__transformData__(transFunList[1:],tables)

    def __loadDataFrame__(self,tables):
        """

        :param tables: dict of dataframes
        :return: None
        """

        for pair in self.dfConnectorsList:
            name = pair["DataFrameName"]
            loadConc = pair["connector"]
            if pair.get("JobExecIdName"):
                fileId = EtlUtils.getUniqueID()
                tables[name] = tables[name].withColumn(pair["JobExecIdName"], lit(fileId))
            loadConc.writeData(self.spark,tables[name])
    def __presistDataFrame__(self,dfInput,StorageLevel=None):
        if not StorageLevel:
            return
        if self.storageMap.get(StorageLevel,None):
            dfInput.persist(self.storageMap[StorageLevel])


class SQLETLJob(ETLJob):
    """
    This class to load all the configuraiton from json file to be passed
    json files will have paths as Tokens to be replaced before using this class
    """
    def __init__(self,jobName,sourceConnectors=None,transList=None,dfConnectorsList=None,*args,**kwargs):
        super().__init__(jobName,sourceConnectors,transList,dfConnectorsList,*args,**kwargs)
    def __transformData__(self,trnsQueriesList,tables):
        """

        :param trnsQueriesList: List of (Map{"Name":"name of dataframe","Query":Query Sring,"StorageLevel": storage level})
        :param tables: list of tables
        :return: list of tables trasformed
        """
        if not trnsQueriesList or len(trnsQueriesList) == 0:
            return tables
        transpair = trnsQueriesList[0]
        tables[transpair["Name"]] = self.spark.sql(transpair["Query"])
        self.__presistDataFrame__(tables[transpair["Name"]],transpair.get("StorageLevel",None))
        tables[transpair["Name"]].createOrReplaceTempView(transpair["Name"])
        return self.__transformData__(trnsQueriesList[1:],tables)


