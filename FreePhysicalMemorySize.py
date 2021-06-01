from subprocess import CalledProcessError

from jmxquery import JMXConnection
from jmxquery import JMXQuery

import argparse


def parsing_arguments():
    p = argparse.ArgumentParser(description="Get memory physical size check")
    p.add_argument("string_host", metavar="'host1:port'",
                   help="an string with the host nad port jmx")
    p.add_argument("-w", help="warning threshold in bytes", type=float, required=True, metavar="warning_threashold")
    p.add_argument("-c", help="critical threshold in bytes", type=float, required=True, metavar="critical_threshold")
    p.add_argument('-u', metavar="user_jmx", help="user name jmx")
    p.add_argument('-pw', metavar="password", help="password for the user jmx")
    arg1 = p.parse_args()
    if (arg1.u and not arg1.pw) or (not arg1.u and arg1.pw):
        p.print_usage()
        p.exit(3, "Error: if you are using authentication jmx, options -u and -pw are requested")

    return arg1, p


if __name__ == "__main__":

    arg, parser = parsing_arguments()
    host = arg.string_host
    critical = arg.c
    warn = arg.w
    type_jmx = "OperatingSystem"
    name = "FreePhysicalMemorySize"

    metric_value_total = 0
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

        jmxQuery = [JMXQuery("java.lang:type=" + type_jmx,
                             metric_name="kafka_cluster_{type}_{name}",
                             metric_labels={"host": host})]

        metrics = jmxConnection.query(jmxQuery)
        for metric in metrics:
            if f"{metric.attribute}" == name:
                used = metric.value
                break
                # print(f"{metric.metric_name}<{metric.attribute}.{metric.attributeKey}> == {metric.value}")

        if not used:
            print("Error - not values found, it cannot be processed the size of the memory")
            exit(3)

        if used <= critical:
            print("CRITICAL - " + name + " - " + str(used))
            exit(2)
        elif used <= warn:
            print("WARNING - " + name + " - " + str(used))
            exit(1)
        else:
            print("OK - " + name + " - " + str(used))
            exit(0)

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
