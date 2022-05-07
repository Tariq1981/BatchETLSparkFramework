import json
from airflow.models import Variable
from airflow.operators.python import PythonOperator

J_PARAM_FILENAME = "J_PARAM_FILENAME"

def readDAGParamFile(fullPath,usedProjectVariablesList=[]):
    """
    This funtion reads the Parameter file which passed as value to 'J_PARAM_FILENAME'. If the file contains parameters
    has the same name as used Airflow variabels in ths DAG, the value in the file will override the Airflow variable.
    :param fullPath: Full pth to the file name
    :param usedProjectVariablesList: List of used Airflow variables in the DAG.
    :return: Dictionary contains the parameters and the used Airflow variables with their names.
    """
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
    """
    This function creates Python task in teh dag to be used to push the used Airflow variables plus the parameters passed.
    :param usedProjectVariablesList: The list of used Aiflow variables in the DAG
    :return: Python task which push the result of readDAGParamFile to XCom.
    """
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
    """
    This function retrieve the value for the paramName form the XCom.
    :param ti: task
    :param paramName: Parameter name
    :return: Teh value for the parameter.
    """
    print(paramName,str(ti))
    params = ti.xcom_pull(task_ids=['set_params'])
    if paramName in params[0]:
        return params[0][paramName]

    print("No !!!!")
    print(str(params))

    return ""



