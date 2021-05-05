import requests
import json
import sys

AMBARI_USER_ID = 'raj_ops'
AMBARI_USER_PW = 'raj_ops'
rest_api = '/api/v1/clusters'
base_url = 'http://localhost:8080'
url = base_url + rest_api
req = requests.get(url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))

json_data=json.loads(req.text)

CLUSTER_NAME=json_data["items"][0]["Clusters"]["cluster_name"]

def ambariREST( restAPI ) :
    url = base_url+restAPI
    r= requests.get(url, auth=(AMBARI_USER_ID, AMBARI_USER_PW))
    return(json.loads(r.text))
    
def getService(SERVICE) :
    restAPI = "/api/v1/clusters/"+CLUSTER_NAME+"/services/"+SERVICE
    json_data =  ambariREST(restAPI)
    return(json_data)

def print_info(service,components) :
    service_json = getService(service)
    alret_summry = service_json['alerts_summary']
    print("#"*120+"\n")
    print("-"+service+" STATUS IS : \n")
    if(service_json['ServiceInfo']['state'] !='STARTED'): 
        print("[state is :] "+service_json['ServiceInfo']['state'])
        print("[alerts summary is] :")
        print("CRITICAL : " + str(alret_summry["CRITICAL"]) +" || "+
               "MAINTENANCE : " + str(alret_summry["MAINTENANCE"])+" || "+
               " OK : " + str(alret_summry["OK"])+" || "+
               " UNKNOWN : " + str(alret_summry["UNKNOWN"])+" || "+
               " WARNING : " + str(alret_summry["WARNING"] )+"\n" )
        for component in components:
          component_json = getService(service+'/components/'+component)['ServiceComponentInfo']
          print("\t -"+component+" info IS : \n")
          print("\t\t[state is :] "+component_json['state'])
          print("\t\t started count : " + str(component_json["started_count"]) +" || "+
                 " total count : " + str(component_json["total_count"])+" || "+
                 " UNKNOWN : " + str(component_json["unknown_count"])+"\n" )
    else:
        print("everything is okay\n")


print_info('ZOOKEEPER',['ZOOKEEPER_SERVER'])

print_info('HDFS',['NAMENODE','JOURNALNODE','SECONDARY_NAMENODE','DATANODE'])

print_info('KAFKA',['KAFKA_BROKER'])

print_info('SPARK2',['SPARK2_CLIENT','SPARK2_JOBHISTORYSERVER','LIVY2_SERVER'])

print_info('HIVE',['HIVE_SERVER','HIVE_CLIENT','HIVE_METASTORE'])

print_info('OOZIE',[])
print("#"*120)