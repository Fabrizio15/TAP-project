from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_json
import pyspark.sql.types as tp
from elasticsearch import Elasticsearch
import time

# without logstash running, we don't have yet data streaming from kafka, so the spark session
# container could run into an error and it has to be restarted, i'm trying to prevent it with 
# this sleep time

print("waiting for logstash to start")
time.sleep(60)

elastic_host="http://elasticsearch:9200"
elastic_index="chess"
kafkaServer="kafkaServer:9092"
topic = "all_games"


# create spark session
sparkConf = SparkConf().set("es.nodes", "elasticsearch") \
                        .set("es.port", "9200")
sc = SparkContext(appName="live_chess", conf=sparkConf)
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

# schema and mapping for data
schema = tp.StructType([
    tp.StructField(name= 'id', dataType= tp.StringType(), nullable= True),
    tp.StructField(name= 'rated', dataType= tp.BooleanType(), nullable= True),
    tp.StructField(name= 'speed', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'status', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'white', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'black', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'white_rating', dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'black_rating', dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'opening_name', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'winner', dataType= tp.StringType(),  nullable= True)
])

es_mapping = {
    "mappings": {
        "properties": 
            {
                "created_at": {"type": "date","format": "yyyy-MM-ddTHH:mm:ss.SSSZ"},
                "id": {"type": "text"},
                "rated": {"type": "text"},
                "speed": {"type": "text"},
                "status": {"type": "text"},
                "white": {"type": "text"},
                "black": {"type": "text"},
                "white_rating": {"type": "integer"},
                "black_rating": {"type": "integer"},
                "opening_name": {"type": "text"},
                "winner": {"type": "text"}
            }
    }
}


es = Elasticsearch(hosts=elastic_host) 

# make an API call to the Elasticsearch cluster
# and have it return a response:

#response = es.indices.create(
#    index=elastic_index,
#    body=es_mapping,
#    ignore=400 # ignore 400 already exists code
#)

#if 'acknowledged' in response:
#    if response['acknowledged'] == True:
#        print ("INDEX MAPPING SUCCESS FOR INDEX:", response['index'])


#Read the data from the kafka streaming and save it in a dataframe
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .load()


df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema=schema).alias("data")) \
    .select("data.*")

# Write the stream to elasticsearch
df.writeStream \
    .option("checkpointLocation", "/save/location") \
    .format("es") \
    .start(elastic_index) \
    .awaitTermination()
