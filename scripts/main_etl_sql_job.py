import argparse
from pyspark.sql import SparkSession
import json
import importlib
import re
from string import Template
from sparkETLJobs.etlutils import EtlUtils

def fullname(o):
    klass = o.__class__
    module = klass.__module__
    if module == 'builtins':
        return klass.__qualname__ # avoid outputs like 'builtins.str'
    return module + '.' + klass.__qualname__

class jobEncoder(json.JSONEncoder):
    def default(self, o) :
        if isinstance(o,SparkSession):
            return {'SparkSessionName': o.sparkContext._conf.get("spark.app.name")}
        return {'{}'.format(fullname(o)): o.__dict__}

def decode_object(o):
    parameters = None
    class_ = None
    for key in o:
        if key == "classname" or key == "jobClassName":
            ls = o[key].split(".")
            packageName = ".".join(ls[0:len(ls)-1])
            module = importlib.import_module(packageName)
            class_ = getattr(module,ls[len(ls)-1])

        elif key == "parameters":
            parameters = o[key]
    if class_ and parameters:
        obj = class_(**parameters)
        return obj
    return o


def endCodeETLJob(job):
    jsonStr = json.dumps(job, cls=jobEncoder, indent=4)
    return jsonStr


def getSQLEtlJobFromJson(jsonFilePath,jsonFileName,*args):
    fullPath = "{}/{}".format(jsonFilePath,jsonFileName)
    with open(fullPath,'r') as file:
        data = file.read()
    dataReplacedParam = replaceTokensWithParams(data,args)
    jobNew = json.loads(dataReplacedParam, object_hook=decode_object,strict=False)
    return jobNew

def replaceTokensWithParams(data,args):
    argsDict=None
    if len(args) > 0:
        argsDict=dict(s.split("=") for s in args)
    newData = re.sub(r"#(\w+?)#", r"{\1}", data)
    replacedData = Template(newData).substitute(**argsDict)
    return replacedData



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Decoding ETL SQL job')
    parser.add_argument('jsonFilePath', metavar='jsonFilePath', type=str, nargs=1,
                        help='The path for the json file')
    parser.add_argument('jsonFileName', metavar='jsonFileName', type=str, nargs=1,
                        help='JSON file name for the ETL job')
    parser.add_argument('jobParams',metavar="jobParams",type=str,nargs='*')
    """
    args = parser.parse_args(['C:/Downloads/ETL Migration/ETLScripts/sample_json_jobs',
                              'sampleJob.json',
                              'P_DELTA_MIR_PATH=/dummy/fff',
                              'P_DB_HIVE_STAGING=DD',
                              'P_DB_HIVE_TEC=TEC'
                              ])
    """
    args = parser.parse_args()
    print(args.jsonFilePath)
    job = getSQLEtlJobFromJson(args.jsonFilePath[0],args.jsonFileName[0],*args.jobParams)
    job.runETL()
    # print(job)
