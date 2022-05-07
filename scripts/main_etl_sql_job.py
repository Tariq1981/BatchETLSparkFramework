import argparse
import json
import importlib
import re
from string import Template

def decode_object(o):
    """
    This function used as callback in the json decoding function to decode custom objects
    :param o: dictionary to be decoded
    :return: either the passed dictionary or object of certain class according to the classname or jobClassName
    """
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


def getSQLEtlJobFromJson(jsonFilePath,jsonFileName,*args):
    """
    This function fire the parsing processes and call the responsible function to replace the parameters with their values
    :param jsonFilePath: Full path for the json file
    :param jsonFileName: json file name
    :param args: parameters to be replaced with their values in the json file
    :return: object from the SQLETLJob
    """
    fullPath = "{}/{}".format(jsonFilePath,jsonFileName)
    with open(fullPath,'r') as file:
        data = file.read()
    dataReplacedParam = replaceTokensWithParams(data,args)
    jobNew = json.loads(dataReplacedParam, object_hook=decode_object,strict=False)
    return jobNew

def replaceTokensWithParams(data,args):
    """
    This function replaces the parameters with their valeus.
    :param data: json data
    :param args: list of parameters with their values (PARAM=VALUE)
    :return: json data with replaced parameters.
    """
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
