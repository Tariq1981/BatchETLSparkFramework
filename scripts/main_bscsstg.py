from sparkETLJobs.bscsstg import BSCSStgBatchJob
import argparse

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
