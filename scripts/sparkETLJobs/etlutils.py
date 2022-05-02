from datetime import datetime
import os
from pyspark.sql import SparkSession

class EtlUtils:
    def __init__(self):
        pass
    @staticmethod
    def getUniqueID():
        currentDate =datetime.today()
        hour = currentDate.hour
        minute = currentDate.minute
        second = currentDate.second
        year = int(currentDate.strftime("%y"))
        sumTime = hour * 3600 + minute * 60 + second
        sumTimeStr = "{:0>5}".format(sumTime)
        juld = "{:0>3}".format(currentDate.timetuple().tm_yday)
        pid = "{:0>5}".format(os.getpid())
        full = str(year) + juld + sumTimeStr +pid
        return full
    @staticmethod
    def getSparkSession(appName):
        spark = SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()
        return spark





