# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/Users/vikasjanardhanan/Softwares/hadoop-2.7.3/
pagerankrowmatrix.jar.name=PageRankRowMatrix-1.0-all.jar
pagerank.jar.path=build/libs/${pagerankrowmatrix.jar.name}
pagerank.job.name=pagerankrowmatrix
local.input=input
local.output=output
# Pseudo-Cluster Execution
hdfs.user.name=vikas
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-5.3.1
aws.region=us-east-1
aws.bucket.name=neu.mr.hw5
aws.subnet.id=subnet-dad053e6
aws.input=input
aws.output=outputs
aws.log.dir=log
aws.num.nodes=5
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	(gradle build && gradle shadowJar)

clean:
	(rm -rf temp && gradle clean)
# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

clear-license:
	-(cd build/libs && zip -d PageRankRowMatrix-1.0-all.jar LICENSE && zip -d PageRankRowMatrix-1.0-all.jar META-INF/LICENSE)

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone: clean-local-output
	${hadoop.root}/bin/hadoop jar ${pagerank.jar.path} ${job.name} ${local.input}/wikipedia-simple-html.bz2 ${local.output}

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${pagerank.jar.path} s3://${aws.bucket.name}/pagerankrowmatrix/
	

# Main EMR launch.
cloud-6:
		$(call run-cluster,"PageRankRowMatrix Cluster",pagerankrowmatrix,${pagerankrowmatrix.jar.name},5)
cloud-11:
		$(call run-cluster,"PageRankRowMatrix Cluster",pagerankrowmatrix,${pagerankrowmatrix.jar.name},10)

define run-cluster
		aws emr create-cluster \
		--name $(1) \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":$(4),"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}/wikipedia--html.0.bz2","s3://${aws.bucket.name}/${aws.input}/wikipedia--html.1.bz2","s3://${aws.bucket.name}/${aws.input}/wikipedia--html.2.bz2","s3://${aws.bucket.name}/${aws.input}/wikipedia--html.3.bz2","s3://${aws.bucket.name}/$(2)/${aws.output}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/$(2)/$(3)","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/$(2)/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate
endef
