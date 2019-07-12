#Progetto per il corso Sistemi e Architetture per Big Data - A.A. 2018/19
## Progetto 2: Analisi dei commenti di articoli pubblicati sul New York Times con Storm/Flink
###(Di Cosmo - Nedia)

Lo scopo del progetto è rispondere ad alcune query riguardanti un dataset relativo ai commenti di articoli `
pubblicati sul New York Times, utilizzando il framework Apache Flink e per la query 2 la libreria client Apache Kafka Streams.
Il dataset contiene dati relativi ai commenti (diretti e indiretti) di articoli pubblicati sul New York Times
dall’1 gennaio 2018 al 18 aprile 2018.

Per eseguire l'applicativo in locale è necessario avere kafka,redis e flink installati all'interno della propria macchina. Una volta attivati kafka,redis e zookeper è sufficiente far partire prima il jar Flink.jar con argomenti [indirizzo Kafka indirizzo Redis] che in questo caso possono essere semplicemente 'localhost' e 'loclahost' (ex: java -jar Flink localhost localhost)
Successivamente eseguire jar Simulator.jar con argomento l'indirizzo di kafka, anche in questo caso è sufficiente passare l'indirizzo locale 'localhost' (ex: java -jar Simulator localhost)

E' possibile inoltre eseguire lapplicativo non in modalità nodo standalone con Apache Flink, ma utilizzando il servizio Cloud per Hadoop Amazon EMR.
In questo caso è necessario innanzitutto avere un account Amazon aws con relative ID chiave di accesso e chiave di accesso segreta che dovranno essere sostittuire all'interno degli scriptn python scriptAWS e launchAWSCluster.
Una volta eseguite queste opreazioni basterà lanciare prima lo script 'scriptAWS' (eseguendo il comando: python scriptAWS) e successivamente 'launchAWSCluster' (eseguendo il comando: pyhton launchAWSCluster).
A questo punto bisognerà accedere alla console di aws e andando su Servizi->ec2->running instances individuare l'istanza di kafka presente in 'eu-central-1c' e inviare, tramite il comando: scp -i 'nomeChiaveAWS.pem' 'inidirizzo della macchina': (ex: scp -i sabd2.pem /home/user/IdeaProjects/Sabd2Project/Data ubuntu@indirizzoIP:), il dataset e il jar Simulator.jar analogamente ed infine connettersi all'istanza di kafka tramite il comando: ssh -i "key.pem" address.

Per collegarsi al cluster in cui è presente flink è necessario eseguire le seguenti operazioni:
	1. Connettersi eseguendo sulla console il comando: ssh -i keypath -C -D 8157 hadoop@masterpublicdns 
	2. Avviare la sesione tramite il comando: flink-yarn-session -n 2 -d
	3. Collegarsi alla dashboard esposta da flink all'indirizzo: http://hadoop@masterpublicdns:8088 
	4. Fare il submit del job inserendo il jar Flink.jar e passando come argomenti l'indirizzo ipv4 pubblico dell'istanza di kafka in cui è stato precedentemente inserito il Simulator.jar e il dataset e l'indirizzo ipv4 pubblico dell'istanza contenente Redis (che è possibile vedere sempre sulla console di amazon aws, andando su Servizi->ec2->running instances)

Infine basterà eseguire il comando [java - jar Simulator.jar localhost] sull'istanza di kafka che inizierà la simulazione dello stream di tuple.



