shell
#!/bin/bash

# Specify the destination path where you want to save the downloaded file
destination_path="/tmp/ozone-repair-jar"

# Create and navigate to the folder
mkdir "$destination_path"
cd "$destination_path"


# Download the JAR file from Maven repository
wget https://repo1.maven.org/maven2/com/google/guava/guava/32.0.0-jre/guava-32.0.0-jre.jar
wget https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.10/slf4j-api-2.0.10.jar
wget https://repo1.maven.org/maven2/org/apache/ratis/ratis-common/3.0.1/ratis-common-3.0.1.jar
wget https://repo1.maven.org/maven2/org/apache/ratis/ratis-thirdparty-misc/1.0.5/ratis-thirdparty-misc-1.0.5.jar
wget https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.19.6/protobuf-java-3.19.6.jar
wget https://repo1.maven.org/maven2/org/rocksdb/rocksdbjni/7.7.3/rocksdbjni-7.7.3.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar
wget https://repo1.maven.org/maven2/org/apache/ozone/hdds-config/1.4.0/hdds-config-1.4.0.jar
wget https://repo1.maven.org/maven2/org/apache/ozone/hdds-common/1.4.0/hdds-common-1.4.0.jar
wget https://repo1.maven.org/maven2/org/apache/ozone/hdds-interface-client/1.4.0/hdds-interface-client-1.4.0.jar
wget https://repo1.maven.org/maven2/org/apache/ozone/ozone-interface-client/1.4.0/ozone-interface-client-1.4.0.jar
wget https://repo1.maven.org/maven2/org/apache/ozone/ozone-common/1.4.0/ozone-common-1.4.0.jar

echo "Downloaded jars successfully!"
