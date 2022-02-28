This project shows a strange behavior that occurs during the generation of the schema when changing the version of avro 1.9.2 to 1.10.2

To reproduce the error change de avro dependency version to version 1.10.2 and run the Maven command: 

For linux:
./mvnw clean verify

For windows:
./mvnw.cmd clean verify

For more details on this, see the AvroSchemaIssueTest class

Issue: https://issues.apache.org/jira/browse/AVRO-3138