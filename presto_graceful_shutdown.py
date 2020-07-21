from datetime import datetime, timezone
from time import sleep
from utility.emr_management import ManageEMR
from airflow.hooks.presto_hook import PrestoHook
from airflow.models import Variable
import requests
from requests import RequestException
from requests.exceptions import InvalidURL
import logging

# logging configuration
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
root = logging.getLogger()
root.setLevel(logging.INFO)


class PrestoGracefulShutdown:

    def __init__(self, timeout=120, interval=180, cluster_details='presto_cluster_details',
                 presto_connection='presto_prd_airflow', worker_port=8080):
        presto_cluster_details = Variable.get(cluster_details, deserialize_json=True)

        self.emr = ManageEMR()
        self.presto_hook = PrestoHook(presto_connection)
        self.cluster_id = presto_cluster_details['cluster_id']
        self.task_od_id = presto_cluster_details['task_od_id']
        self.presto_max_count = presto_cluster_details['presto_max_cnt']
        self.presto_min_count = presto_cluster_details['presto_min_cnt']
        self.presto_weekend_min_count = presto_cluster_details['presto_weekend_min_cnt']
        self.presto_weekend_max_count = presto_cluster_details['presto_weekend_max_cnt']
        self.shutdown_timeout = timeout
        self.monitor_interval = interval
        self.worker_port = worker_port

    def get_emr_node_ips(self, instance_group_id):
        instances = self.emr.get_running_instances(self.cluster_id, instance_group_id)
        instance_ips = [instance['PrivateIpAddress'] for instance in instances]
        return instance_ips

    def get_nodes_to_shutdown(self, node_ips, nodes_shutdown_count):
        node_sql = """
        select url_extract_host(nodes.http_uri) ip_address, nodes.state node_state, tasks.* from system.runtime.nodes nodes
        left join (select t.node_id, count(t.stage_id) stage_cnt, min(t.start) running_since from system.runtime.tasks t
        inner join system.runtime.queries q on q.query_id = t.query_id
        where t.state='RUNNING' -- and regexp_like("stage_id", '[0|1]$')
        and q.user not in ('airflow', 'ec2-user')
        group by t.node_id) tasks on nodes.node_id = tasks.node_id
        where nodes.coordinator=false and nodes.state = 'active'
        and url_extract_host(nodes.http_uri) in ({0})
        order by stage_cnt asc, running_since desc limit %s;
        """.format("'{0}'".format("', '".join(node_ips)))

        nodes_df = self.presto_hook.get_pandas_df(node_sql, [nodes_shutdown_count])
        print(nodes_df)
        return nodes_df['ip_address'].tolist()

    @staticmethod
    def initiate_presto_shutdown(nodes, port):
        successful_nodes = list()
        for node in nodes:
            url = 'http://{0}:{1}/v1/info/state'.format(node, port)

            header = {'Content-Type': 'application/json'}
            response = None
            try:
                response = requests.put(url, data='"SHUTTING_DOWN"', headers=header)
            except InvalidURL as e:
                print('Invalid URL: Seems already shutdown: {}'.format(str(e)))
            except RequestException as e:
                print('Request failed: {}'.format(str(e)))
            else:
                print('Shut down request sent successfully for: {}'.format(node))
                successful_nodes.append(node)

            print(response.text)
            print(response.status_code)
            sleep(1)
        return successful_nodes

    def get_inactive_nodes(self, nodes, include_shutting_down=False):
        condition = " and state='active'" if include_shutting_down else ""
        active_nodes_sql = """select url_extract_host(nodes.http_uri) ip_address from system.runtime.nodes nodes
        where nodes.coordinator=false {}""".format(condition)

        active_nodes_df = self.presto_hook.get_pandas_df(active_nodes_sql)
        active_nodes = active_nodes_df['ip_address'].tolist()
        logging.info('Active Worker Count: %s', len(active_nodes))

        inactive_nodes = list(filter(lambda n: n not in active_nodes, nodes))
        logging.info('Total inactive nodes: %s', len(inactive_nodes))
        return inactive_nodes

    def gracefully_shutdown_nodes(self, instance_group_id, shutdown_count, nodes=None):
        emr_nodes = self.get_emr_node_ips(instance_group_id) if nodes is None else nodes
        inactive_nodes = self.get_inactive_nodes(emr_nodes, include_shutting_down=True)

        shutting_down_nodes = inactive_nodes
        if shutdown_count > 0:
            nodes_to_shutdown = self.get_nodes_to_shutdown(emr_nodes, shutdown_count)
            logging.info('Nodes to shutdown: %s', nodes_to_shutdown)
            shutting_down_nodes.extend(self.initiate_presto_shutdown(nodes_to_shutdown, self.worker_port))

        if len(shutting_down_nodes) > 0:
            logging.info('Monitoring and terminating nodes: %s', shutting_down_nodes)
            self.monitor_and_terminate_shutdown_nodes(instance_group_id, shutting_down_nodes)
        else:
            logging.info('No nodes to shutdown')

    def monitor_and_terminate_shutdown_nodes(self, instance_group_id, shutting_down_nodes):
        nodes_to_monitor = shutting_down_nodes
        start_time = datetime.now(timezone.utc)

        while True:
            shutdown_nodes = self.get_inactive_nodes(nodes_to_monitor)
            if len(shutdown_nodes) > 0:
                logging.info('Terminating Shutdown Nodes: %s', shutdown_nodes)
                sleep(60)
                self.emr.scale_down_by_ip_address(self.cluster_id, instance_group_id, shutdown_nodes)

                nodes_to_monitor = [node for node in nodes_to_monitor if node not in shutdown_nodes]
                if len(nodes_to_monitor) == 0:
                    logging.info('All nodes have been terminated gracefully')
                    break

            logging.info('Monitoring %s nodes: %s', len(nodes_to_monitor), nodes_to_monitor)

            diff_minutes = int(((datetime.now(timezone.utc) - start_time).total_seconds())/60)
            logging.info('Time spent: %s, Timeout: %s', diff_minutes, self.shutdown_timeout)
            if diff_minutes > self.shutdown_timeout:
                logging.info('Terminating remaining nodes as time spent(%s) exceeds timeout(%s)', diff_minutes, self.shutdown_timeout)
                self.emr.scale_down_by_ip_address(self.cluster_id, instance_group_id, nodes_to_monitor)
                break
            sleep(self.monitor_interval)

    def scale_down(self):
        emr_nodes = self.get_emr_node_ips(self.task_od_id)
        current_count = len(emr_nodes)
        required_count = self.presto_weekend_min_count if datetime.today().weekday() in [4, 5, 6] else self.presto_min_count

        shutdown_count = current_count - required_count
        if shutdown_count <= 0:
            logging.info('Scale down not required as required_count(%s) >= current_count(%s)', required_count, current_count)
            return True

        logging.info('Gracefully terminating %s nodes', shutdown_count)
        self.gracefully_shutdown_nodes(self.task_od_id, shutdown_count, emr_nodes)

    def scale_up(self):
        emr_nodes = self.get_emr_node_ips(self.task_od_id)
        current_count = len(emr_nodes)
        required_count = self.presto_weekend_max_count if datetime.today().weekday() in [5, 6] else self.presto_max_count

        increase_count = required_count - current_count
        if increase_count <= 0:
            logging.info('Scale up not required as required_count(%s) <= current_count(%s)', required_count, current_count)
            return True

        logging.info('Increasing nodes to %s nodes', required_count)
        self.emr.modify_instance_count(self.cluster_id, self.task_od_id, required_count)


def gracefully_shutdown_od_nodes(number_of_nodes, shutdown_timeout=120, monitor_interval=180):
    pgs = PrestoGracefulShutdown(timeout=shutdown_timeout, interval=monitor_interval)
    pgs.gracefully_shutdown_nodes(pgs.task_od_id, number_of_nodes)
