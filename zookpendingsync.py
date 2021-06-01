import urllib.request
import sys
import json

script_name = "zookpendingsync.py"


def usage():
    print(script_name + ' <ip_zookeeper1:port,ip_zookeeper2:port>')


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print("List of zookeepers is missing")
        usage()
        exit(1)
    list = sys.argv[1].split(",")
    if len(list) == 0 :
        print('bad format for list of zookeeper')
        usage()
        exit(1)
    no_leader = True
    for host in list:
        try:
            url = 'http://'+host+'/commands/monitor'
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req) as response:
                page = response.read()
                rjson = json.loads(page)
                if rjson.get("server_state") == "leader":
                    pending_sync = rjson.get("pending_syncs")
                    print("no_leader: False , pending_syncs: "+str(pending_sync))
                    no_leader = False
                    break
        except urllib.error.URLError as e:
            print(e.code)

    if no_leader:
        print("no_leader: True , pending_syncs: -1")