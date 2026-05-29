Perfect  let’s make it super simple — daily steps (copy-paste style) 


OR  ------------------------------------------------->>> If Error Come 

docker-compose down

docker volume prune


E:\FinStream-Cloud-Data-Platform\FinStream-Cloud-Data-Platform\docker>docker volume prune
WARNING! This will remove anonymous local volumes not used by at least one container.
Are you sure you want to continue? [y/N] y

E:\FinStream-Cloud-Data-Platform\FinStream-Cloud-Data-Platform\docker>docker rm -f kafka zookeeper postgres spark

E:\FinStream-Cloud-Data-Platform\FinStream-Cloud-Data-Platform\docker>docker-compose up -d
[+] up 5/5
 ✔ Network docker_default Created                                                                                                                           0.0s ✔ Container spark        Started                                                                                                                           1.7s ✔ Container postgres     Started                                                                                                                           1.6s ✔ Container zookeeper    Started                                                                                                                           1.4s ✔ Container kafka        Started                                                                                                                           1.9s
E:\FinStream-Cloud-Data-Platform\FinStream-Cloud-Data-Platform\docker>docker logs kafka

 COMPLETE RUN STEPS (every time)
STEP 1: Start Docker

 Go to docker folder:

cd docker

Run:

docker-compose up -d

 This starts:

Kafka 
Zookeeper 
Spark 
PostgreSQL 
 STEP 2: Start ingestion (VERY IMPORTANT)

 Go to project root:

cd ..

Run:

python ingestion/api_ingestion.py
E:\FinStream-Cloud-Data-Platform\FinStream-Cloud-Data-Platform>python -m ingestion.api_ingestion
Sent: {'id': 'bitcoin', 'symbol': 'btc', 'price': 68647, 'market_cap': 1373844591680, 'timestamp': 1775585691.6828597}

 This sends data → Kafka

 STEP 3: Start Spark job

Run:

docker exec -it spark bash

Then:

cd /opt/spark-apps


Then run:

/opt/spark/bin/spark-submit \
--conf spark.jars.ivy=/tmp/.ivy \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
spark_streaming_processing.py
 FINAL FLOW
Step 1 → Start containers
Step 2 → Send data to Kafka
Step 3 → Process data using Spark
 EXPECTED OUTPUT
-------------------------------------------
Batch: 0
-------------------------------------------
{"id":"bitcoin","price":...}
 IMPORTANT NOTES

✔ Keep ingestion running
✔ Keep Spark running
✔ Don’t close terminal

 SUPER SHORT VERSION (remember this)
1. docker-compose up -d
2. python ingestion/api_ingestion.py
3. spark-submit
 OPTIONAL (easy mode)

If you want:
 I can make 1 command to run everything automatically

Just say:
 “auto setup” 



################# new lates


 STEP 3 — FULL RESET (VERY IMPORTANT)
cd docker
docker-compose down -v
docker-compose up -d

 -v removes old broken configs

 STEP 4 — CREATE TOPIC AGAIN
docker exec -it kafka bash
kafka-topics --create \
--topic transactions \
--bootstrap-server kafka:29092 \
--partitions 1 \
--replication-factor 1
 STEP 5 — RUN PIPELINE
Terminal 1 (INGESTION)
python -m ingestion.api_ingestion
Terminal 2 (SPARK)
docker exec -it spark bash
/opt/spark/bin/spark-submit \
--conf spark.jars.ivy=/tmp/.ivy \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
spark/spark_streaming_processing.py
 EXPECTED OUTPUT
 Processing batch: 0
 Rows: 10
 Writing to DB...
 WRITE SUCCESS
