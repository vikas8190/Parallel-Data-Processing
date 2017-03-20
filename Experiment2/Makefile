# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/Users/vikasjanardhanan/Softwares/hadoop-2.7.3/
nocombiner.jar.name=nocombiner-1.0-all.jar
combiner.jar.name=combiner-1.0-all.jar
inmapcombiner.jar.name=inmapcombiner-1.0-all.jar
secondarysort.jar.name=secondarysort-1.0-all.jar
nocombiner.jar.path=nocombiner/build/libs/${nocombiner.jar.name}
combiner.jar.path=combiner/build/libs/${combiner.jar.name}
inmapcombiner.jar.path=inmapcombiner/build/libs/${inmapcombiner.jar.name}
secondarysort.jar.path=secondarysort/build/libs/${secondarysort.jar.name}
nocombiner.job.name=nocombiner
local.input=input
local.output=output
# Pseudo-Cluster Execution
hdfs.user.name=vikas
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-5.3.1
aws.region=us-east-1
aws.bucket.name=neu.mr.hw2
aws.subnet.id=subnet-dad053e6
aws.input=input
aws.output=outputs
aws.log.dir=log
aws.num.nodes=5
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	(cd nocombiner && gradle build && gradle shadowJar)
	(cd combiner && gradle build && gradle shadowJar)
	(cd inmapcombiner && gradle build && gradle shadowJar)
	(cd secondarysort && gradle build && gradle shadowJar)

clean:
	(cd nocombiner && gradle clean)
	(cd combiner && gradle clean)
	(cd inmapcombiner && gradle clean)
	(cd secondarysort && gradle clean)
# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

clear-license:
	-(cd nocombiner/build/libs && zip -d nocombiner-1.0-all.jar LICENSE && zip -d nocombiner-1.0-all.jar META-INF/LICENSE)
	-(cd combiner/build/libs && zip -d combiner-1.0-all.jar LICENSE && zip -d combiner-1.0-all.jar META-INF/LICENSE)
	-(cd inmapcombiner/build/libs && zip -d inmapcombiner-1.0-all.jar LICENSE && zip -d inmapcombiner-1.0-all.jar META-INF/LICENSE)
	-(cd secondarysort/build/libs && zip -d secondarysort-1.0-all.jar LICENSE && zip -d secondarysort-1.0-all.jar META-INF/LICENSE)

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone-nocombiner: clean-local-output
	${hadoop.root}/bin/hadoop jar ${nocombiner.jar.path} ${job.name} ${local.input}/1991.csv ${local.output}
alone-combiner: clean-local-output
	${hadoop.root}/bin/hadoop jar ${combiner.jar.path} ${job.name} ${local.input}/1991.csv ${local.output}
alone-inmapcombiner: clean-local-output
	${hadoop.root}/bin/hadoop jar ${inmapcombiner.jar.path} ${job.name} ${local.input}/1991.csv ${local.output}
alone-secondarysort: clean-local-output
	${hadoop.root}/bin/hadoop jar ${secondarysort.jar.path} ${job.name} ${local.input}/1880.csv ${local.input}/1881.csv ${local.input}/1882.csv ${local.input}/1883.csv ${local.input}/1884.csv ${local.input}/1885.csv ${local.input}/1886.csv ${local.input}/1887.csv ${local.input}/1888.csv ${local.input}/1889.csv ${local.output}

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
	aws s3 cp ${combiner.jar.path} s3://${aws.bucket.name}/combiner/
	aws s3 cp ${nocombiner.jar.path} s3://${aws.bucket.name}/nocombiner/
	aws s3 cp ${inmapcombiner.jar.path} s3://${aws.bucket.name}/inmapcombiner/
	aws s3 cp ${secondarysort.jar.path} s3://${aws.bucket.name}/secondarysort/
	

# Main EMR launch.
cloud-combiner:
		$(call run-cluster-1,"Combiner Cluster",combiner,${combiner.jar.name})
cloud-nocombiner:
		$(call run-cluster-1,"No Combiner Cluster",nocombiner,${nocombiner.jar.name})
cloud-inmapcombiner:
		$(call run-cluster-1,"InMap Combiner Cluster",inmapcombiner,${inmapcombiner.jar.name})
cloud-secondarysort:
		$(call run-cluster-2,"SecondarySort Cluster",secondarysort,${secondarysort.jar.name})
define run-cluster-1
		aws emr create-cluster \
		--name $(1) \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}/1991.csv","s3://${aws.bucket.name}/$(2)/${aws.output}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/$(2)/$(3)","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/$(2)/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging
		--auto-terminate
endef
define run-cluster-2
		aws emr create-cluster \
		--name $(1) \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}/1880.csv","s3://${aws.bucket.name}/${aws.input}/1881.csv","s3://${aws.bucket.name}/${aws.input}/1882.csv","s3://${aws.bucket.name}/${aws.input}/1883.csv","s3://${aws.bucket.name}/${aws.input}/1884.csv","s3://${aws.bucket.name}/${aws.input}/1885.csv","s3://${aws.bucket.name}/${aws.input}/1886.csv","s3://${aws.bucket.name}/${aws.input}/1887.csv","s3://${aws.bucket.name}/${aws.input}/1888.csv","s3://${aws.bucket.name}/${aws.input}/1889.csv","s3://${aws.bucket.name}/$(2)/${aws.output}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/$(2)/$(3)","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/$(2)/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging
		--auto-terminate
endef
