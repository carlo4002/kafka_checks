from subprocess import CalledProcessError

from jmxquery import JMXConnection
from jmxquery import JMXQuery

import argparse
from os import path


def host_list(string_hosts):
    comma = ","
    two_points = ":"
    if string_hosts.find(comma) != -1 or string_hosts.find(two_points) != -1 in str:
        return string_hosts.split(",")
    else:
        if not path.exists(string_hosts):
            parser.print_usage()
            parser.exit(3, "ERROR: file name %s does not exist" % string_hosts)

        file = open(string_hosts, 'r')
        return [line.rstrip('\n') for line in file.readlines()]


def parsing_arguments():
    parser = argparse.ArgumentParser(description="Get the unclean Election per second of the cluster")
    parser.add_argument("string_hosts", metavar="<host1:port,host2:port...> | <filename>",
                        help="list of host with ports or file with list of hosts with ports")
    parser.add_argument('-u', metavar="user_jmx", help="user name jmx")
    parser.add_argument('-pw', metavar="password", help="password for the user jmx")
    arg1 = parser.parse_args()
    if (arg1.u and not arg1.pw) or (not arg1.u and arg1.pw):
        parser.print_usage()
        parser.exit(3, "Error: if you are using auth jmx, options -u and -pw are requested")

    return arg1, parser


if __name__ == "__main__":

    arg, parser = parsing_arguments()
    hosts = host_list(arg.string_hosts)

    type_jmx = "ControllerStats"
    name = "UncleanLeaderElectionsPerSec"

    metric_value_total = 0
    for host in hosts:
        try:
            if len(host.split(':')) == 1:
                parser.print_usage()
                parser.exit(3, "Error : port missing in string '%s'" % host)

            if arg.u:
                jmxConnection = JMXConnection("service:jmx:rmi:///jndi/rmi://" + host + "/jmxrmi",
                                              jmx_username=arg.u,
                                              jmx_password=arg.pw)
            else:
                jmxConnection = JMXConnection("service:jmx:rmi:///jndi/rmi://" + host + "/jmxrmi")

            jmxQuery = [JMXQuery("kafka.controller:type=" + type_jmx + ",name=" + name,
                                 metric_name="kafka_cluster_{type}_{name}",
                                 metric_labels={"host": host})]

            metrics = jmxConnection.query(jmxQuery)
            for metric in metrics:
                if metric.attribute == "Count":
                    # print(f"{metric.metric_name}<{metric.metric_labels}> == {metric.value}")
                    metric_value_total += metric.value
                    break

        except TimeoutError as e:
            err = "Error : connexion failed '%s'" % host
            print(err)
            exit(3)
            raise Exception(err)
        except CalledProcessError as e:
            err = "Error : connexion failed '%s'" % host
            print(err)
            exit(3)
            raise Exception(err)

    if metric_value_total == 0:
        print("OK - " + name + " - " + str(metric_value_total))
        exit(0)
    else:
        print("WARNING - " + name + " - " + str(metric_value_total))
        exit(1)
