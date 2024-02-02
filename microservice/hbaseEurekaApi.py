from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
from typing import Union
from extract_to_hbase import HBaseClient
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
import py_eureka_client.eureka_client as eureka_client

transport = TSocket.TSocket('localhost', 9090)

protocol = TBinaryProtocol.TBinaryProtocol(transport)

app = FastAPI()

def setEureka():
    server_host = "localhost"
    server_port = 2400
    eureka_client.init(eureka_server="http://127.0.0.1:8761",
                       app_name="hbaseapi_service",
                       # 当前组件的主机名，可选参数，如果不填写会自动计算一个，如果服务和 eureka 服务器部署在同一台机器，请必须填写，否则会计算出 127.0.0.1
                       instance_host=server_host,
                       instance_port=server_port)

@app.get('/{word}')
async def get_root(word: str):
    return word

@app.put("/analysis/{dataset_name}")
async def extract_to_HBase(dataset_name: str):
    hbase_client = HBaseClient(protocol)
    transport.open()
    hbase_client.extract_to_hbase(dataset_name)
    transport.close()
    return "Put results into HBase successfully!"

@app.get("/analysis/{dataset_name}/")
async def get_results(dataset_name: str):
    hbase_client = HBaseClient(protocol)
    transport.open()
    result_dict = hbase_client.get_data(dataset_name)
    transport.close()
    return JSONResponse(result_dict)

@app.get("/analysis/{dataset_name}/{diffraction_index}")
async def get_single_result(dataset_name: str, diffraction_index: str):
    hbase_client = HBaseClient(protocol)
    transport.open()
    result_dict = hbase_client.get_data(dataset_name=dataset_name, col_family=diffraction_index)
    transport.close()
    return JSONResponse(result_dict)

if __name__ == "__main__":
    setEureka()
    uvicorn.run('hbaseApi:app', host='0.0.0.0', port=2400, reload=True)