aws emr create-cluster \
    --release-label emr-6.7.0 \
    --applications Name=Ganglia Name=Presto \
    --service-role EMR_DefaultRole  \
    --ec2-attributes KeyName=xxx,SubnetId=subnet-xxxx,InstanceProfile=EMR_EC2_DefaultRole,EmrManagedMasterSecurityGroup=sg-xxxx,EmrManagedSlaveSecurityGroup=sg-xxx \
    --instance-groups file://./instancegroupconfig.json \
    --auto-scaling-role EMR_AutoScaling_DefaultRole \
    --configurations file://./configurations.json \