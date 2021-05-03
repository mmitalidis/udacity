# Sparkify Data Lake on AWS EMR 
 


## Scope of the project

Sparkify is a fictional music streaming startup company. They plan to analyze user data in order to extract useful insights.
The scope of this project is to parse the company data and store them in a data lake in S3.

The amount of data makes it impractical to analyze on a single node, so [Apache Spark](https://spark.apache.org/) will be used to
extract them from S3, transform to a star schema and load them back to S3 in a separate bucket.

We are going to use the AWS Elastic Map Reduce ([EMR](https://aws.amazon.com/emr/)) for creating our Spark cluster in the cloud and
processing the data.



## Getting Started

The steps for running the ETL pipeline are the following:

1. Create an AWS S3 bucket for storing the output (see section: **Data I/O**)
2. Edit the `etl.py` file and update it with the target bucket
3. Create a EC2 key-pair in the AWS region where the EMR cluster will be launched (see section: **EC2 Key Pair**)
4. Create an AWS EMR cluster (see section: **Creating the Cluster**)
5. Execute the pipeline on the cluster (see section: **Executing the ETL process**)



## Running the pipeline

### Data I/O

The input data reside in an [Amazon S3](https://aws.amazon.com/s3/) bucket provided by the Udacity platform. The bucket is: `s3://udacity-dend/`.
We want to process the data and then store them in a new bucket, which will be accessed later from BI applications.

We use the `aws` cli tool to create this new bucket. Specifically, with the command:

```bash
aws s3 mb s3://udacity-mm-spark-etl-test
```

Please note that each bucket name is unique across all AWS accounts, so the bucket name in command above needs to be replaced with 
an appropriate name.


### EC2 Key Pair

In order to be able to `ssh` to the master node of the EMR cluster, we need to create an EC2 key pair.
The key pair should be in the same AWS region with the one we are going to create the EMR cluster.


This can easily be done from the AWS console, by navigating to the EC2 section and selecting `Key Pairs`.
For this project, the key pair is named `spark-cluster-key`.



### Creating the Cluster

There are a few way of creating an EMR cluster. One can use the AWS console, or more conveniently use the AWS cli tool for creating the cluster.
Assuming we have stored our AWS credentials in `~/.aws`, we can simply do and create a new EMR cluster with 3 nodes.
One of them will be the master and the other 2 will be worker nodes.

Also, note how we specify the key name that we are going to use in order to ssh to the master node.
```bash
aws emr create-cluster --name spark-cluster --use-default-roles --release-label emr-5.28.0 --instance-count 3 --applications Name=Spark  --ec2-attributes KeyName=spark-cluster-key --instance-type m5.xlarge
```

As soon as the cluster is ready we can check the addresses of the master node and of the slaves, so that we can ssh into master.
For example
```bash
aws ec2 describe-instances --filters Name=instance-state-name,Values=running --query 'Reservations[*].Instances[*].{Instance:InstanceId,Name:SecurityGroups[0].GroupName,Address:PublicDnsName}'
---------------------------------------------------------------------------------------------------------
|                                           DescribeInstances                                           |
+----------------------------------------------------+----------------------+---------------------------+
|                       Address                      |      Instance        |           Name            |
+----------------------------------------------------+----------------------+---------------------------+
|  ec2-44-234-35-7.us-west-2.compute.amazonaws.com   |  i-069dc5aaa9a664064 |  ElasticMapReduce-master  |
|  ec2-34-222-211-77.us-west-2.compute.amazonaws.com |  i-02b1155698e56afb9 |  ElasticMapReduce-slave   |
|  ec2-34-223-54-225.us-west-2.compute.amazonaws.com |  i-0f660902143b2bbcf |  ElasticMapReduce-slave   |
+----------------------------------------------------+----------------------+---------------------------+
```

#### Deployment using AWS Cloudformation

An alternative method for creating the cluster is to define a Cloudformation template ,
together with a VPC. This will make the Infrastructure reproducible.

The python package [troposphere](https://github.com/cloudtools/troposphere) is very useful for this purpose.
It allows one to create a Python template of the AWS deployment and then easily export that to 
`json` or `yaml`. These final artifacts can then be uploaded to AWS, which will create all of the resources for us.


What's interesting about this particular deployment is that we explicitly specify an additional 
[security group](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html) that we use for the master node, so that we can ssh into it. 
Additionally, we choose not to specify a [Network Access Control List](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-network-acls.html),
since the default one, that allows all trafic to leave the VPC works. We manage the permissions with security groups.


Finally, we avoid using the default [EMR EC2 role](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role-for-ec2.html),
because it's too permissive. It gives access to many AWS services which we don't use.
Instead we provide read-only access to the Udacity bucket with the input data and full-access to the S3 bucket where
we are going to store the results. Of course, the ARNs of the source and target buckets ater parametrizable as inputs to the Cloudformation template.



### Executing the ETL process

#### Copy the ETL file to Master

After obtaining the Public IP of the EMR master node we continue by uploading the `etl.py` file to it.
Using the AWS cli, this should look like the following:
```bash
scp -i ~/.aws/spark-cluster-key.pem etl.py hadoop@ec2-44-234-35-7.us-west-2.compute.amazonaws.com:/home/hadoop/etl.py
```


#### Run the the job

Next we use ssh to run the job. We can use the command `spark-submit` in order to run it:
```bash
ssh -t -i ~/.aws/spark-cluster-key.pem hadoop@ec2-44-234-35-7.us-west-2.compute.amazonaws.com "PYSPARK_PYTHON=/usr/bin/python3 spark-submit etl.py"
```

Note, that the `etl.py` file runs only on `python 3.x`, due to `typing`, so one has to specify the `PYSPARK_PYTHON` environment variable to be
`PYSPARK_PYTHON=/usr/bin/python3`.



#### Monitor the process

As explained in the Udacity course and the [AWS docs](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-ssh-tunnel.html)
we are going to use dynamic port forwarding so that we can use are private key pair and 
access the Spark UI running on the EMR cluster.

This can be easily set up by running:
```bash
ssh -i ~/.aws/spark-cluster-key.pem -N -D 8157 hadoop@ec2-34-211-81-216.us-west-2.compute.amazonaws.com
```

We will also need to setup a proxy for Google Chrome so that we can use the ssh tunnel for connecting to Spark UI.
Again, the [AWS docs](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-proxy.html)
provide a guide.




#### Verify the data

Finally, once the process finishes, we terminate the cluster and verify that the data has been written in the target S3 bucket.
Indeed, we can see that's the case:
```bash
% aws s3 ls s3://udacity-mm-spark-etl-test/    
                           PRE artists_table.parquet/
                           PRE data/
                           PRE song_table.parquet/
                           PRE songplays_table.parquet/
                           PRE times_table.parquet/
                           PRE users_table.parquet/
```




## Data Lake Design


### Purpose of the database

Over the course of the previous projects, we examined how the data produced from the app usage, combined with static data about
the songs artists from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/) can be used to create a database
suitable for OLAP processing.


### Database schema design and ETL pipeline

The design database schema needs to make it suitable for Bussiness Intelligence (BI) application. These applications need to 
provide an overview of the data and enable answering business specific questions. In order to achieve this will denormalize
the data partially, thus avoiding complex join operations.


### Derivation of tables

As discussed above, the schema of the database is a star schema. Thus, our tables are being split into a fact table, `songplays_table` in this case
and dimension tables. Below, we describe the steps for producing these tables using Spark.

It is more straightforward to describe the dimension tables, since they are all being derived from a single data source.

Specifically, after parsing the `song_data` json files we extract the `songs_table` and `artists_table`. These tables
contain information about the the songs and the artists found in the Million Song Dataset. In this step, we need to make
sure that we don't add any duplicate rows, or rows that contain `null` ids.

For example, one artist will have probably written multiple songs, so we need to drop all of the duplicate rows in the artist table.

Similarly, we produce the `users_table` and `times_table` by parsing app usage data. Again, we derive these tables from the actions a
user has taken, i.e. page visits. This means that we will have multiple entries for each user and need drop duplicates accordingly.
It is also worth keeping in mind that the user profile will get updated over time. For example, the `level` can potentially change from
`free` to `paid`. We drop duplicates for the `users_table` by selecting the most recent profile entry for each user. This could change,
depending on the requirements of the analysis, and we could decide to store versions of the users profile for more in-depth analysis.

The `times_table` contains accessible information (hour, day, week etc.) about the time of each event to avoid complex SQL logic in
BI apps.

Finally, for the fact table `songplays_table`, we need to join three separate tables, namely `songs_table`, `artists_table` and `log_data`
in order to extract all the relevant information.

### Defining the table schema

In this particular project, our tables have a small number of columns in total, so it is relatively easy to specify the schema (Spark data types)
in the code and then provide that as input to the read function. Otherwise, Spark will require one extra pass of the data in order to infer
the schema, as described in the [features](https://github.com/databricks/spark-csv#features).


Additionally, when defining the schema, we only specify the data types and not the nullable parameter. Spark will 
[ignore](https://forums.databricks.com/questions/33566/nullable-fields-in-schema-are-not-inforced-by-appl.html) the value of this parameter,
if the semantics of the underlying data source (which is `json` in our example) does not enforce nullability.

