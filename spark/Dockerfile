# Spark image for ShopLake: official Apache Spark + the jars it needs to
# (1) read/write Iceberg tables through the REST catalog (iceberg-spark-runtime),
# (2) talk S3 to MinIO for Iceberg files (iceberg-aws-bundle = AWS SDK v2),
# (3) read raw CSVs from MinIO with the s3a:// filesystem (hadoop-aws, plus the
#     two AWS SDK pieces S3A needs that the Iceberg bundle leaves out).
# Jars are baked in at build time, so jobs start fast and work offline.
# Versions: Spark 4.1 <-> iceberg-spark-runtime-4.1; Spark 4.1 ships Hadoop 3.4.2.
FROM apache/spark:4.1.4-python3

ARG MAVEN=https://repo1.maven.org/maven2
ADD --chown=spark:spark ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar /opt/spark/jars/
ADD --chown=spark:spark ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar /opt/spark/jars/
ADD --chown=spark:spark ${MAVEN}/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar /opt/spark/jars/
# Same AWS SDK version as inside iceberg-aws-bundle-1.12.0 (2.54.17)
ADD --chown=spark:spark ${MAVEN}/software/amazon/awssdk/s3-transfer-manager/2.54.17/s3-transfer-manager-2.54.17.jar /opt/spark/jars/
ADD --chown=spark:spark ${MAVEN}/software/amazon/awssdk/apache-client/2.54.17/apache-client-2.54.17.jar /opt/spark/jars/

ENV PATH=/opt/spark/bin:$PATH
WORKDIR /opt/shoplake
