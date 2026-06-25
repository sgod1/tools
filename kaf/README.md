## tools
Kaf tool

This is Kafka troubleshooting tool to put messages on Kafka topics and evaluate performance with threading and blocking strategies.<br/>

*producer.properties*, and *message.json* files are required in the current directory.<br/>

To run:
```
java -jar kaf-1.0.0.jar
```

*producer.properties* is Kafka producer configuration file.</br>

SSL truststore of type *PEM* is expected.<br/>
Paste CA certificates as one line for the *ssl.truststore.certificates* property.<br/>

*topic* property is requied.<br/>

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
```
