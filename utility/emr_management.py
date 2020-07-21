import boto3
import time


class ManageEMR:

    def __init__(self, region='us-west-2'):
        self.__emr_client = boto3.client('emr', region_name=region)

    def get_emr_id(self, emr_name):
        page_iterator = self.__emr_client.get_paginator('list_clusters').paginate(
            ClusterStates=['RUNNING', 'WAITING']
        )
        clusters_active = []
        for page in page_iterator:
            clusters_active.extend(page['Clusters'])

        clusters_found = list(filter(lambda cluster: cluster['Name'] == emr_name, clusters_active))

        if len(clusters_found) == 0:
            raise Exception('No Clusters found with name: {0}'.format(emr_name))
        elif len(clusters_found) > 1:
            raise Exception('Multiple active clusters found for: {0}'.format(emr_name))

        return clusters_found[0]['Id']

    def get_instance_group(self, emr_id, instance_group_type):
        instance_groups = None
        try:
            instance_groups = self.__emr_client.list_instance_groups(ClusterId=emr_id)['InstanceGroups']
        except Exception as e:
            time.sleep(60)
            instance_groups = self.__emr_client.list_instance_groups(ClusterId=emr_id)['InstanceGroups']

        instance_groups_found = list(
            filter(lambda instance_grp: instance_grp['InstanceGroupType'] == instance_group_type, instance_groups))

        if len(instance_groups_found) == 0:
            raise Exception('No instance groups of type: {0} found for: {1}'.format(instance_group_type, emr_id))
        elif len(instance_groups_found) > 1:
            raise Exception('Multiple instance groups of type: {0} found for: {1}'.format(instance_group_type, emr_id))

        return instance_groups_found[0]

    def increase_instances(self, cluster_id, instance_group, requested_count):
        current_count = instance_group['RunningInstanceCount']
        if current_count >= requested_count:
            print('Requested count: {0} is same or less than current count: {1}'.format(requested_count, current_count))
        else:
            print('Increasing instance count to {0}'.format(requested_count))
            self.__emr_client.modify_instance_groups(ClusterId=cluster_id, InstanceGroups=[
                {'InstanceGroupId': instance_group['Id'], 'InstanceCount': requested_count}])

    def put_auto_scale_yarn_apps(self, cluster_id, instance_group_id, min_capacity, max_capacity,
                       add_nodes=2, add_threshold=30, add_periods=1, add_cooldown=120,
                       remove_nodes=2, remove_threshold=95, remove_periods=2, remove_cooldown=120,
                                 remove_apps_nodes=10, remove_apps_periods=2, remove_apps_cooldown=120):
        response = self.__emr_client.put_auto_scaling_policy(
            ClusterId=cluster_id,
            InstanceGroupId=instance_group_id,
            AutoScalingPolicy={
                'Constraints': {
                    'MinCapacity': min_capacity,
                    'MaxCapacity': max_capacity
                },
                'Rules': [
                    {
                        'Name': 'scale_in_yarn',
                        'Description': 'scale_in_yarn',
                        'Action': {
                            'SimpleScalingPolicyConfiguration': {
                                'ScalingAdjustment': -1 * remove_nodes,
                                'CoolDown': remove_cooldown
                            }
                        },
                        'Trigger': {
                            'CloudWatchAlarmDefinition': {
                                'ComparisonOperator': 'GREATER_THAN_OR_EQUAL',
                                'EvaluationPeriods': remove_periods,
                                'MetricName': 'YARNMemoryAvailablePercentage',
                                'Period': 300,
                                'Threshold': remove_threshold,
                                'Unit': 'PERCENT'
                            }
                        }
                    },
                    {
                        'Name': 'scale_in_apps_running',
                        'Description': 'scale_in_apps_running',
                        'Action': {
                            'SimpleScalingPolicyConfiguration': {
                                'ScalingAdjustment': -1 * remove_apps_nodes,
                                'CoolDown': remove_apps_cooldown
                            }
                        },
                        'Trigger': {
                            'CloudWatchAlarmDefinition': {
                                'ComparisonOperator': 'LESS_THAN_OR_EQUAL',
                                'EvaluationPeriods': remove_apps_periods,
                                'MetricName': 'AppsRunning',
                                'Period': 300,
                                'Threshold': 0,
                                'Unit': 'COUNT'
                            }
                        }
                    },
                    {
                        'Name': 'scale_out_yarn',
                        'Description': 'scale_out_yarn',
                        'Action': {
                            'SimpleScalingPolicyConfiguration': {
                                'ScalingAdjustment': add_nodes,
                                'CoolDown': add_cooldown
                            }
                        },
                        'Trigger': {
                            'CloudWatchAlarmDefinition': {
                                'ComparisonOperator': 'LESS_THAN_OR_EQUAL',
                                'EvaluationPeriods': add_periods,
                                'MetricName': 'YARNMemoryAvailablePercentage',
                                'Period': 300,
                                'Threshold': add_threshold,
                                'Unit': 'PERCENT'
                            }
                        }
                    }
                ]
            }
        )
        print(response)

    def remove_auto_scaling_policy(self, cluster_id, instance_group_id):
        response = self.__emr_client.remove_auto_scaling_policy(
            ClusterId=cluster_id,
            InstanceGroupId=instance_group_id
        )
        print(response)

    def modify_instance_count(self, cluster_id, instance_group_id, requested_count):
        response = self.__emr_client.modify_instance_groups(
            ClusterId=cluster_id,
            InstanceGroups=[
                {
                    'InstanceGroupId': instance_group_id,
                    'InstanceCount': requested_count
                }
            ]
        )
        print(response)

    def get_running_instances(self, cluster_id, instance_group_id):
        page_iterator = self.__emr_client.get_paginator('list_instances').paginate(
            ClusterId=cluster_id,
            InstanceGroupId=instance_group_id,
            InstanceStates=['RUNNING']
        )

        instances_running = []
        for page in page_iterator:
            instances_running.extend(page['Instances'])

        return instances_running

    def get_ec2_instance_ids(self, cluster_id, instance_group_id, ip_addresses):
        instances_running = self.get_running_instances(cluster_id, instance_group_id)
        instance_ids = [instance['Ec2InstanceId'] for instance in instances_running if instance['PrivateIpAddress'] in ip_addresses]
        return instance_ids

    def scale_down_by_ip_address(self, cluster_id, instance_group_id, ip_addresses):
        ec2_instance_ids = self.get_ec2_instance_ids(cluster_id, instance_group_id, ip_addresses)
        print('Terminating instances:', ip_addresses, ec2_instance_ids)
        response = self.__emr_client.modify_instance_groups(
            ClusterId=cluster_id,
            InstanceGroups=[
                {
                    'InstanceGroupId': instance_group_id,
                    'EC2InstanceIdsToTerminate': ec2_instance_ids
                }
            ]
        )
        print(response)
