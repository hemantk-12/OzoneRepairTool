shell
#!/bin/bash

# Check if the correct number of parameters are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <dbDir> <snapshotsFilePath> <dryRun>"
    exit 1
fi

# Assign the parameters to variables
dbDir=$1
filePath=$2
dryRun=$3

# Run the Java JAR file with the provided parameters
java -cp /tmp/ozone-repair-jar/OzoneRepairTool-1.0-SNAPSHOT.jar:/tmp/ozone-repair-jar/guava-32.0.0-jre.jar:/tmp/ozone-repair-jar/ratis-thirdparty-misc-1.0.5.jar:/tmp/ozone-repair-jar/slf4j-api-2.0.10.jar:/tmp/ozone-repair-jar/hadoop-common-3.3.6.jar:/tmp/ozone-repair-jar/hdds-config-1.4.0.jar:/tmp/ozone-repair-jar/ratis-common-3.0.1.jar:/tmp/ozone-repair-jar/hdds-interface-client-1.4.0.jar:/tmp/ozone-repair-jar/ozone-interface-client-1.4.0.jar:/tmp/ozone-repair-jar/protobuf-java-3.19.6.jar:/tmp/ozone-repair-jar/hdds-common-1.4.0.jar:/tmp/ozone-repair-jar//rocksdbjni-7.7.3.jar:/tmp/ozone-repair-jar/ozone-common-1.4.0.jar -DdryRun=true ozone.repair.tool.DeleteSnapshot "$dbDir" "$filePath" "$dryRun"
