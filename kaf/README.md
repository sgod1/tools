## tools
Kaf tool

This is Kafka troubleshooting tool to send and receive messages on Kafka topics and evaluate performance with threading and blocking strategies.<br/>

To build: *jdk 21*
```
mvnw clean install
```

Note, that *jdk 25* is not supported with current *3.4.0* version of *kafka-client*.<br/>

*producer.properties* and *message.json*, or *consumer.properties* files are required in the current directory.<br/>

*Sending Messages*

To send messages and save output to the file:
```
java -jar kaf-3.0.0.jar prod [msgs/per second] [duration in seconds] 2>&1 | tee test.out
```

With no arguments, 1 msg will be sent in 1 batch.<br/>

*producer.properties* is Kafka producer configuration file.</br>

SSL truststore of type *PEM* is expected.<br/>
Place CA certificates in *ssl.truststore.certificates* property.<br/>

*topic* property is required.<br/>

To set threading strategy, set *executors.factory* property
```
executors.factory=virtual
#executors.factory=cached
#executors.factory=fixed
#executors.fixedThreadPoolSize=50
```

*producer.propeties* example
```
bootstrap.servers=topdogs-roky-ibm-egw-rt-cp4i.apps.roky.szesto.io:443,topdogs-roky-ibm-egw-rt-1-cp4i.apps.roky.szesto.io:443,topdogs-roky-ibm-egw-rt-2-cp4i.apps.roky.szesto.io:443,topdogs-roky-ibm-egw-rt-3-cp4i.apps.roky.szesto.io:443,topdogs-roky-ibm-egw-rt-4-cp4i.apps.roky.szesto.io:443,topdogs-toby-ibm-egw-rt-cp4i.apps.roky.szesto.io:443,topdogs-toby-ibm-egw-rt-1-cp4i.apps.roky.szesto.io:443,topdogs-toby-ibm-egw-rt-2-cp4i.apps.roky.szesto.io:443,topdogs-toby-ibm-egw-rt-3-cp4i.apps.roky.szesto.io:443,topdogs-toby-ibm-egw-rt-4-cp4i.apps.roky.szesto.io:443

ssl.truststore.certificates=-----BEGIN CERTIFICATE-----\nMIIRwhV91TE\n-----END CERTIFICATE-----\n-----BEGIN CERTIFICATE-----\nMIIDIDCCe1Xgj\n-----END CERTIFICATE-----
ssl.truststore.type=PEM
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="password";

topic=zorro

client.id=szestoio
acks=all
enable.idempotence=true

executors.factory=virtual
#executors.factory=cached
#executors.factory=fixed
#executors.fixedThreadPoolSize=50
```
*Receiving messages*

To receive messages and save output to the file:
```
java -jar kaf-3.0.0.jar cons [number-of-messages]
```
Receive loop will receive at least `number-of-messages` if specified, otherwise receive until interrupted.<br/>

Consumer commits records after each poll.<br/>

*consumer.properties* is Kafka consumer configuration file.</br>

SSL truststore of type *PEM* is expected.<br/>
Place CA certificates in *ssl.truststore.certificates* property.<br/>

*topic* property is required.<br/>

*group.id* property to name consumer group is required.<br/>

*consumer.properties* example.
```
bootstrap.servers=topdogs-roky-ibm-egw-rt-cp4i.apps.roky.szesto.io:443,topdogs-roky-ibm-egw-rt-1-cp4i.apps.roky.szesto.io:443,topdogs-roky-ibm-egw-rt-2-cp4i.apps.roky.szesto.io:443,topdogs-roky-ibm-egw-rt-3-cp4i.apps.roky.szesto.io:443,topdogs-roky-ibm-egw-rt-4-cp4i.apps.roky.szesto.io:443,topdogs-toby-ibm-egw-rt-cp4i.apps.roky.szesto.io:443,topdogs-toby-ibm-egw-rt-1-cp4i.apps.roky.szesto.io:443,topdogs-toby-ibm-egw-rt-2-cp4i.apps.roky.szesto.io:443,topdogs-toby-ibm-egw-rt-3-cp4i.apps.roky.szesto.io:443,topdogs-toby-ibm-egw-rt-4-cp4i.apps.roky.szesto.io:443

ssl.truststore.certificates=-----BEGIN CERTIFICATE-----\nMIIRwhV91TE\n-----END CERTIFICATE-----\n-----BEGIN CERTIFICATE-----\nMIIDIDCCe1Xgj\n-----END CERTIFICATE-----
ssl.truststore.type=PEM
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="password";

topic=zorro

client.id=roky
group.id=beefeaters
auto.offset.reset=latest
```
