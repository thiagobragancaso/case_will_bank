FROM apache/hive:4.0.0

USER root
RUN apt-get -y update && apt-get -y install curl
RUN curl -Lo /opt/hive/lib/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar 
RUN curl -Lo /opt/hive/lib/hadoop-aws-3.3.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar
RUN curl -Lo /opt/hive/lib/aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar
CMD /bin/bash
