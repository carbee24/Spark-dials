
import py_eureka_client.eureka_client as eureka_client
import json


eureka_client.init(eureka_server="http://127.0.0.1:8761",
                       app_name="hbase_api_service_consumer",
                       # 当前组件的主机名，可选参数，如果不填写会自动计算一个，如果服务和 eureka 服务器部署在同一台机器，请必须填写，否则会计算出 127.0.0.1
                       instance_host='localhost')
# result = eureka_client.do_service('hbase_api_service', '/analysis/C2-sum/', method = 'PUT')
# print(result)
result = eureka_client.do_service('hbase_api_service', '/analysis/C2-sum/')
json_result = json.loads(result)
print(json.dumps(json_result, sort_keys=True, indent=4, separators=(',', ':')))




