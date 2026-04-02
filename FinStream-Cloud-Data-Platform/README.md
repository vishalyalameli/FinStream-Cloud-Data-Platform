Perfect 👍 let’s make it super simple — daily steps (copy-paste style) 🔥

🚀 ✅ COMPLETE RUN STEPS (every time)
🔹 STEP 1: Start Docker

📍 Go to docker folder:

cd docker

Run:

docker-compose up -d

👉 This starts:

Kafka ✅
Zookeeper ✅
Spark ✅
PostgreSQL ✅
🔹 STEP 2: Start ingestion (VERY IMPORTANT)

📍 Go to project root:

cd ..

Run:

python ingestion/api_ingestion.py

👉 This sends data → Kafka

🔹 STEP 3: Start Spark job

Run:

docker exec -it spark bash

Then:

cd /opt/spark-apps

Then run:

/opt/spark/bin/spark-submit \
--conf spark.jars.ivy=/tmp/.ivy \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
spark_streaming_processing.py
🎯 FINAL FLOW
Step 1 → Start containers
Step 2 → Send data to Kafka
Step 3 → Process data using Spark
🔥 EXPECTED OUTPUT
-------------------------------------------
Batch: 0
-------------------------------------------
{"id":"bitcoin","price":...}
⚠️ IMPORTANT NOTES

✔ Keep ingestion running
✔ Keep Spark running
✔ Don’t close terminal

🧠 SUPER SHORT VERSION (remember this)
1. docker-compose up -d
2. python ingestion/api_ingestion.py
3. spark-submit
🚀 OPTIONAL (easy mode)

If you want:
👉 I can make 1 command to run everything automatically

Just say:
👉 “auto setup” 🔥