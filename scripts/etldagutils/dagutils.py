import json
from airflow.models import Variable
from airflow.operators.python import PythonOperator

J_PARAM_FILENAME = "J_PARAM_FILENAME"

def readDAGParamFile(fullPath,usedProjectVariablesList=[]):
    print(fullPath)
    jobParams = {}
    for pParam in usedProjectVariablesList:
        jobParams[pParam] = Variable.get(pParam)
    try:
        with open(fullPath, "r") as file:
            jParamsDict = json.load(file)
            for jParam in jParamsDict:
                jobParams[jParam] = jParamsDict[jParam]
    except Exception as e:
        print("Error in Reading Parameters File. Loading will be as there is no special parameter file !!!!,"+fullPath+","+str(e))
    return jobParams

def getParamPythonOperator(usedProjectVariablesList=[]):

    P_PATH_JOB_PARAM_PATH = Variable.get("P_PATH_JOB_PARAM_PATH")
    pythonSetParams = PythonOperator(
        task_id="set_params",
        python_callable=readDAGParamFile,
        op_args=[P_PATH_JOB_PARAM_PATH+"/{{ dag_run.conf['J_PARAM_FILENAME'] }}",
                 usedProjectVariablesList],
        do_xcom_push=True
    )
    return pythonSetParams

def getParamValue(ti,paramName):
    print(paramName,str(ti))
    params = ti.xcom_pull(task_ids=['set_params'])
    if paramName in params[0]:
        return params[0][paramName]

    print("No !!!!")
    print(str(params))

    return ""



