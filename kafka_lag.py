import urllib.request
import sys
import json

script_name = "kafka_lag_check.py"


def usage():
    print(script_name + ' <group_name>')


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print("group name needed")
        usage()
        exit(1)
group = sys.argv[1]
cluster = "local"
try:
   url='http://localhost:8000/v3/kafka/' + cluster + '/consumer/' + group + '/lag'
   req = urllib.request.Request(url)

   with urllib.request.urlopen(req) as response:
       page = response.read()
       rjson = json.loads(page)
       current_lag = rjson.get("status").get("maxlag").get('current_lag')
       topic = rjson.get("status").get("maxlag").get('topic')
       status = rjson.get("status").get("maxlag").get('status')
       print("topic: "+topic+"; status: "+status+"; current_lag:"+str(current_lag))
except urllib.error.URLError as e:
   print(e.reason)
