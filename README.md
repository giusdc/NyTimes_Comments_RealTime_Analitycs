### Progetto per il corso Sistemi e Architetture per Big Data - A.A. 2018/19
## Progetto 2: Analisi dei commenti di articoli pubblicati sul New York Times con Storm/Flink
### (Di Cosmo Giuseppe - Nedia Salvatore)

Lo scopo del progetto è rispondere ad alcune query riguardanti un dataset relativo ai commenti di articoli
pubblicati sul *New York Times*, utilizzando il framework Apache Flink e per la query 2 la libreria client Apache Kafka Streams.
Il dataset contiene dati relativi ai commenti (diretti e indiretti) di articoli pubblicati sul New York Times
dall’1 gennaio 2018 al 18 aprile 2018.

* Per eseguire l'applicativo in locale è necessario avere kafka,zookeeper redis e flink installati all'interno della propria macchina. Una volta attivati kafka,redis e zookeper è sufficiente:
    * Far partire prima il jar Flink.jar con argomenti indirizzo Kafka indirizzo Redis, che in questo caso possono essere semplicemente localhost e loclahost ex: ``` java -jar allquery.jar localhost localhost```
    * Eseguire il jar Simulator.jar con argomento l'indirizzo di kafka, anche in questo caso è sufficiente passare l'indirizzo locale localhost ex: ```java -jar simulator localhost```
    
* E' possibile inoltre eseguire lapplicativo non in modalità nodo standalone con Apache Flink, ma utilizzando il servizio Cloud per Hadoop Amazon EMR.
In questo caso è necessario innanzitutto avere un account Amazon aws con relative ID chiave di accesso e chiave di accesso segreta, che dovranno essere sostittuire all'interno degli script python scriptAWS e launchAWSCluster. Creare una coppia di chiavi tramite la console Amazon EC2, da specificare anch'essa nello script scriptAWS.
Una volta eseguite queste operazioni basterà: 
   * Lanciare prima lo script 'scriptAWS' (eseguendo il comando:``` python scriptAWS```)  
  * Modificare nello script 'launchAWSCluster' il campo *'Ec2SubnetId': 'subnted ID XXX* inserendo l'apposita subnetId di kafka creata con lo script precedente
  * Lanciare lo script 'launchAWSCluster eseguendo il comando: ```pyhton launchAWSCluster```
  * Accedere alla console di aws e andando su Servizi->ec2->running instances, individuare l'istanza di kafka presente in 'eu-central-1c' e inviare, tramite il comando: ```scp -i keypath datasetPath/jarsPath ubuntu@indirizzoIPkafka:```, il dataset  e il jar simulator.jar
   * Connettersi all'istanza di kafka tramite il comando: ```ssh -i keyPath address```.
   * Creare la cartella data e inserire all'interno il file contenente il dataset
   * Collegarsi al cluster in cui è presente flink eseguendo sulla console il comando: ```ssh -i keypath -C -D 8157 hadoop@masterpublicdns``` 
   * Avviare la sesione tramite il comando: ```flink-yarn-session -n 2 -d```
   * Collegarsi alla dashboard esposta da flink all'indirizzo: <http://hadoop@masterpublicdns:8088> 
   * Fare il submit del job inserendo il jar allquery.jar e passando come argomenti l'indirizzo ipv4 pubblico dell'istanza di kafka,in cui è stato precedentemente inserito il simulator.jar e il dataset, e l'indirizzo ipv4 pubblico dell'istanza contenente Redis (che è possibile vedere sempre sulla console di amazon aws, andando su Servizi->ec2->running instances)
   * Infine basterà eseguire il comando ```java -jar simulator.jar localhost``` sull'istanza di kafka che inizierà la simulazione dello stream di tuple.
   
* Per eseguire la query 2 dell'applicativo tramite la libreria client Apache Kafka Streams in locale è necessario:<br>
 Anche in questo caso avere kafka e zookeeper installati all'interno della propria macchina 
    * Attivare kafka e zookeper
    * Far partire prima il jar KafkaStreams.jar con il comando ```java -jar kafkaStream.jar localhost```
    * Infine far partire il jar del simulatore tramite il comando ```java -jar simulator.jar localhost```

* Per eseguire la query 2 dell'applicativo tramite la libreria client Apache Kafka Streams è necessario:<br>
Anche in questo avere un account Amazon aws con relative ID chiave di accesso e chiave di accesso segreta che dovranno essere sostittuire all'interno dello script python scriptAWS e poi eseguire le seguenti operazioni:

    * Lanciare lo script 'scriptAWS' tramite il comando ```python scriptAWS```
    * Accedere alla console di aws su Servizi->ec2->running instances e sceglire una istanza di kafka
    * Trasferire dataset, simulator.jar, kafkaStreams.jar tramite il comando: ```scp -i keypath datasetPath/jarPath ubuntu@indirizzoIPkafka:```
    * Connettersi all'istanza di kafka tramite il comando: ```ssh -i keypath ubuntu@indirizzoIPkafka:```
    * Creare la cartella data e inserire al suo interno il dataset
    * Lanciare il simulatore tramite il comando ```java -jar Simulator.jar localhost```
    * Lanciare (su un altro terminale) il DSP tramite il comando ```java -jar kafkaStreams.jar localhost```


