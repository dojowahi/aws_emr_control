#!/bin/bash
# Author:Ankur Wahi


source /home/hadoop/scripts/global_var.sh

cluster_nm=$1
action=$2

today=`date +%Y-%m-%d`
log_file=${log_dir}/presto_node_${action}_${today}.log

rm -rf ${log_file}

cluster_id=$(aws emr list-clusters --region us-west-2 --active  --query 'Clusters[?Name==`'${cluster_nm}'`].Id' --output text)

cluster_cnt=`echo ${cluster_id} | wc -w`

if [ ${cluster_cnt} -eq 1 ]
then
         cluster_dns=$(aws emr describe-cluster --region us-west-2 --cluster-id ${cluster_id} --query 'Cluster.MasterPublicDnsName' --output text)
         master_ip=`echo ${cluster_dns} | cut -d'.' -f1 | cut -d'-' -f2- | sed -e "s|-|.|g"`
         run_task=$(aws emr list-instances --region us-west-2 --cluster-id ${cluster_id} --instance-group-types "TASK" --instance-states RUNNING --query Instances[*].Ec2InstanceId --output text)
         run_task_cnt=`echo $run_task | wc -w`
         instance_task_id=$(aws emr describe-cluster --region us-west-2 --cluster-id ${cluster_id} --query 'Cluster.InstanceGroups[?InstanceGroupType==`TASK`].Id' --output text)
fi

echo "Scaling ${action} cluster ${cluster_dns}" >>${log_file}

# Run presto query to check running count

if [ ${action} == "down" ]
then

        run_presto_cnt=$(/home/hadoop/scripts/presto --server ${master_ip}:8080 --catalog system --execute "select count(*) from system.runtime.queries where state='RUNNING'" | sed -e 's/"//g')
#       run_presto_cnt=0
        let running_cnt=$run_presto_cnt-1
        echo "Queries running in presto: $running_cnt" >>${log_file}

        if [[ ( $run_presto_cnt -eq 0 ) && ( $run_task_cnt -ne $presto_min_cnt ) ]]
        then
                let kill_cnt=$run_task_cnt-$presto_min_cnt >>${log_file}
                if [ $kill_cnt -lt 0 ]
                then
                        echo "No nodes to kill" >>${log_file}
                        cat ${log_file} | mailx -s "Presto ${action} Auto Scaling for ${cluster_nm} is complete" ${email_me}
                        exit 0
                fi
                echo "Kill ${kill_cnt} task nodes"
                fmt_kill_task_id=`echo ${run_task}| cut -d' ' -f1-${kill_cnt} |sed 's/ \{1,\}/,/g'`
                aws emr modify-instance-groups --region us-west-2 --cluster-id ${cluster_id} --instance-groups InstanceGroupId=$instance_task_id,EC2InstanceIdsToTerminate=${fmt_kill_task_id}

        else
                echo "Nodes will not be killed as query count id :$running_cnt and task node count is : $presto_min_cnt" >>${log_file}
        fi

else
        aws emr modify-instance-groups --region us-west-2 --cluster-id ${cluster_id} --instance-groups InstanceGroupId=${instance_task_id},InstanceCount=${presto_max_cnt}
fi

cat ${log_file} | mailx -s "Presto ${action} Auto Scaling for ${cluster_nm} is complete" ${email_me}
