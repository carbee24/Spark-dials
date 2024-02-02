from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
from typing import Union
from spark_job_process import sparkJobProcesser
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
import py_eureka_client.eureka_client as eureka_client

transport = TSocket.TSocket('localhost', 9090)

protocol = TBinaryProtocol.TBinaryProtocol(transport)

app = FastAPI()

def setEureka():
    server_host = "localhost"
    server_port = 2401
    eureka_client.init(eureka_server="http://127.0.0.1:8761",
                       app_name="sparkapi_service",
                       # 当前组件的主机名，可选参数，如果不填写会自动计算一个，如果服务和 eureka 服务器部署在同一台机器，请必须填写，否则会计算出 127.0.0.1
                       instance_host=server_host,
                       instance_port=server_port)

@app.get('/{word}')
async def get_root(word: str):
    return word

@app.get('/spark/status/{dataset_name}')
async def status(dataset_name: str):
    spark_job_client = sparkJobProcesser(protocol)
    transport.open()
    result = spark_job_client.get_job_status(dataset_name)
    transport.close()
    return result





if __name__ == "__main__":
    setEureka()
    uvicorn.run('sparkJobApi:app', host='0.0.0.0', port=2401, reload=True)