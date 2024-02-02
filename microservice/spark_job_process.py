import os
import time
import requests
from hbase.Hbase import Client, Mutation

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

transport = TSocket.TSocket('localhost', 9090)

protocol = TBinaryProtocol.TBinaryProtocol(transport)

class sparkJobProcesser(Client):

    def __init__(self, protocol):
        super(sparkJobProcesser, self).__init__(protocol)

    def submit_job(self, master_file):
        pid = os.fork()
        if pid == 0:
            os.system('ccp4-python /home/Data/local/spark_dials/pipeline/spark_dials_pipeline_yarn_cluster_test.py %s'%master_file)
        while True:
            t = 0
            if t < 10:
                res = requests.get('http://localhost:18080/api/v1/applications?status=running')
                if res:
                    for app_info in res.json():
                        if app_info['name'] == '5-5-1_1':
                            column = 'job_info:appId'
                            value = app_info['id']
                            mutation1 = Mutation(column=column, value=value)
                            # self.mutateRow('Spark_Jobs', '5-5-1_1', mutation)
                            column = 'status_info:submit'
                            value = 'success'
                            mutation2 = Mutation(column=column, value=value)
                            # self.mutateRow('Spark_Jobs', '5-5-1_1', mutation)
                            column = 'status_info:result'
                            value = 'null value'
                            mutation3 = Mutation(column=column, value=value)
                            mutations = [mutation1, mutation2, mutation3]
                            self.mutateRow('Spark_Jobs', '5-5-1_1', mutations)
                            return "Spark application summited successfully!"
                t = t + 0.1
                time.sleep(0.1)
        return "Spark application submission failed"
    
    def get_job_status(self, datasetname):
        app_id_list = self.get('Spark_Jobs', datasetname, 'job_info:appId')
        app_id = app_id_list[0].value
        # return app_id.value
        res = requests.get('http://localhost:18080/api/v1/applications?status=running')
        if res:
            for app_info in res.json():
                if app_info['id'] == app_id:
                    return 'Spark application is running......'
        res = requests.get('http://localhost:18080/api/v1/applications/' + app_id + '/jobs')
        res = res.json()
        if len(res) == 3:
            for result in res:
                if result['status'] == 'SUCCEEDED':
                    continue
                else:
                    column = 'status_info:result'
                    value = 'failed'
                    mutation = [Mutation(column=column, value=value)]
                    self.mutateRow('Spark_Jobs', datasetname, mutation)
                    return "Spark application FAILED!"
            column = 'status_info:result'
            value = 'successed'
            mutation = [Mutation(column=column, value=value)]
            self.mutateRow('Spark_Jobs', datasetname, mutation)
            return "Spark application SUCCEEDED!"
        column = 'status_info:result'
        value = 'failed'
        mutation = [Mutation(column=column, value=value)]
        self.mutateRow('Spark_Jobs', datasetname, mutation)
        return "Spark application FAILED!"



if __name__ == '__main__':
    spark_job_processer = sparkJobProcesser(protocol)
    transport.open()
    result = spark_job_processer.get_job_status('5-5-1_1')
    transport.close()
    print(result)