"""
Creates a Cloudformation EMR template using the troposphere library.
Requires: https://github.com/cloudtools/troposphere
"""
import troposphere.emr as emr
import troposphere.iam as iam
from awacs.aws import Action, Allow, PolicyDocument, Statement, Principal
from troposphere import (
    Join,
    GetAtt,
    Parameter,
    Ref,
    Template,
    Output,
)
from troposphere.constants import KEY_PAIR_NAME, M5_XLARGE
from troposphere.ec2 import (
    VPC,
    InternetGateway,
    Route,
    RouteTable,
    SecurityGroup,
    SecurityGroupRule,
    Subnet,
    SubnetRouteTableAssociation,
    VPCGatewayAttachment,
)
from troposphere.iam import Policy


def main() -> None:
    """Writes a JSON CloudFormation template for the EMR cluster"""
    template = Template()
    template.set_version("2010-09-09")
    add_network_resources(template)
    add_emr_cluster(template)

    with open("emr_template.json", "w") as f:
        f.write(template.to_json())


def add_network_resources(template: Template) -> None:
    """
    Creates AWS network resources and provides ssh access.

    Mostly copied from the library examples:
    https://github.com/cloudtools/troposphere/blob/2.7.1/examples/VPC_single_instance_in_subnet.py
    """
    vpc = template.add_resource(VPC("VPC", CidrBlock="10.0.0.0/16"))
    subnet = template.add_resource(
        Subnet(
            "Subnet",
            CidrBlock="10.0.0.0/24",
            VpcId=Ref(vpc),
        )
    )

    internetGateway = template.add_resource(InternetGateway("InternetGateway"))

    gatewayAttachment = template.add_resource(
        VPCGatewayAttachment(
            "AttachGateway", VpcId=Ref(vpc), InternetGatewayId=Ref(internetGateway)
        )
    )

    routeTable = template.add_resource(RouteTable("RouteTable", VpcId=Ref(vpc)))

    route = template.add_resource(
        Route(
            "Route",
            DependsOn="AttachGateway",
            GatewayId=Ref(internetGateway),
            DestinationCidrBlock="0.0.0.0/0",
            RouteTableId=Ref(routeTable),
        )
    )

    subnetRouteTableAssociation = template.add_resource(
        SubnetRouteTableAssociation(
            "SubnetRouteTableAssociation",
            SubnetId=Ref(subnet),
            RouteTableId=Ref(routeTable),
        )
    )

    instanceSecurityGroup = template.add_resource(
        SecurityGroup(
            "InstanceSecurityGroup",
            GroupDescription="Enable SSH access via port 22",
            SecurityGroupIngress=[
                SecurityGroupRule(
                    IpProtocol="tcp", FromPort="22", ToPort="22", CidrIp="0.0.0.0/0"
                ),
            ],
            VpcId=Ref(vpc),
        )
    )


def add_emr_cluster(template: Template) -> None:
    """Adds an EMR cluster with S3 access to the template."""
    keyname = template.add_parameter(
        Parameter(
            "Ec2KeyName",
            Description="Name of an existing EC2 KeyPair to enable SSH to the instances",
            Type=KEY_PAIR_NAME,
        )
    )
    source_s3_bucket_arn = template.add_parameter(
        Parameter(
            "SourceS3BucketArn",
            Description="ARN of the S3 bucket that the EMR cluster will read data from",
            Type="String",
            Default="arn:aws:s3:::udacity-dend",
        )
    )

    target_s3_bucket_arn = template.add_parameter(
        Parameter(
            "TargetS3BucketArn",
            Type="String",
            Description="ARN of the S3 bucket that the EMR cluster will read data from",
            Default="arn:aws:s3:::udacity-mm-spark-etl-test",
        )
    )

    emr_service_role = template.add_resource(
        iam.Role(
            "EMRServiceRole",
            AssumeRolePolicyDocument=PolicyDocument(
                Statement=[
                    Statement(
                        Effect=Allow,
                        Principal=Principal(
                            "Service", ["elasticmapreduce.amazonaws.com"]
                        ),
                        Action=[Action("sts", "AssumeRole")],
                    )
                ]
            ),
            ManagedPolicyArns=[
                "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
            ],
        )
    )

    emr_job_flow_role = template.add_resource(
        iam.Role(
            "EMRJobFlowRole",
            AssumeRolePolicyDocument=PolicyDocument(
                Statement=[
                    Statement(
                        Effect=Allow,
                        Principal=Principal("Service", ["ec2.amazonaws.com"]),
                        Action=[Action("sts", "AssumeRole")],
                    )
                ],
            ),
            Policies=[
                Policy(
                    PolicyName="S3TargetFullAccess",
                    PolicyDocument=PolicyDocument(
                        Statement=[
                            Statement(
                                Effect=Allow,
                                Resource=Join("", [Ref(target_s3_bucket_arn), "*"]),
                                Action=[Action("s3", "*")],
                            )
                        ]
                    ),
                ),
                Policy(
                    PolicyName="S3SourceReadOnlyAccess",
                    PolicyDocument=PolicyDocument(
                        Statement=[
                            Statement(
                                Effect=Allow,
                                Resource=Join("", [Ref(source_s3_bucket_arn), "*"]),
                                Action=[Action("s3", "Get*"), Action("s3", "List*")],
                            )
                        ]
                    ),
                ),
            ],
        )
    )

    emr_instance_profile = template.add_resource(
        iam.InstanceProfile("EMRInstanceProfile", Roles=[Ref(emr_job_flow_role)])
    )

    cluster = template.add_resource(
        emr.Cluster(
            "cluster",
            Name="Spark Cluster",
            ReleaseLabel="emr-5.28.0",
            JobFlowRole=Ref(emr_instance_profile),
            ServiceRole=Ref(emr_service_role),
            Instances=emr.JobFlowInstancesConfig(
                Ec2KeyName=Ref(keyname),
                Ec2SubnetId=Ref(template.resources["Subnet"]),
                AdditionalMasterSecurityGroups=[
                    Ref(template.resources["InstanceSecurityGroup"])
                ],
                MasterInstanceGroup=emr.InstanceGroupConfigProperty(
                    Name="Master Instance",
                    InstanceCount="1",
                    InstanceType=M5_XLARGE,
                    Market="ON_DEMAND",
                ),
                CoreInstanceGroup=emr.InstanceGroupConfigProperty(
                    Name="Core Instance",
                    Market="ON_DEMAND",
                    InstanceCount="10",
                    InstanceType=M5_XLARGE,
                ),
            ),
            Applications=[
                emr.Application(Name="Spark"),
            ],
            VisibleToAllUsers="true",
        )
    )

    template.add_output(
        Output(
            "KeyName",
            Description="Use the following key to ssh to the EMR cluster.",
            Value=Ref(keyname),
        )
    )

    template.add_output(
        Output(
            "MasterNodeEndpoint",
            Description="Access the EMR cluster from the following endpoint (the user should be hadoop)",
            Value=GetAtt("cluster", "MasterPublicDNS"),
        ),
    )


if __name__ == "__main__":
    main()
