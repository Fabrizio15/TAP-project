from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import IndexToString
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col
from pyspark.ml import Pipeline 
from elasticsearch import Elasticsearch
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
import pyspark.sql.types as tp
from pyspark.ml.classification import OneVsRest
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.functions import from_json


elastic_host="http://elasticsearch:9200"
elastic_index="chess_real_time"
kafkaServer="kafkaServer:9092"
topic = "real_time"

# features used for this model
FEATURES = [
    'white_rating_index',
    'black_rating_index'
    ]

# schema and mapping of data
schema = tp.StructType([
    tp.StructField(name= 'id', dataType= tp.StringType(), nullable= True),
    tp.StructField(name= 'speed', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'status', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'white', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'black', dataType= tp.StringType(),  nullable= True),
    tp.StructField(name= 'white_rating', dataType= tp.IntegerType(),  nullable= True),
    tp.StructField(name= 'black_rating', dataType= tp.IntegerType(),  nullable= True)
])

es_mapping = {
    "mappings": {
        "properties": 
            {
                "created_at": {"type": "date","format": "yyyy-MM-ddTHH:mm:ss.SSSZ"},
                "id": {"type": "text"},
                "speed": {"type": "text"},
                "status": {"type": "text"},
                "white": {"type": "text"},
                "black": {"type": "text"},
                "white_rating": {"type": "integer"},
                "black_rating": {"type": "integer"},
            }
    }
}

# to send data to elasticsearch
def write_to_es(df):
    df.write.format("org.elasticsearch.spark.sql") \
                .option("es.nodes", "elasticsearch") \
                .option("es.port", "9200") \
                .option("es.resource", elastic_index) \
                .mode("append") \
                .save()


def create_pipeline(df):

    # stringindexer is used to have numeric input for the model
    indexer_1 = StringIndexer(inputCol = 'winner', outputCol = 'winner_index', stringOrderType="alphabetAsc", handleInvalid = 'keep')
    indexer_2 = StringIndexer(inputCol="white_rating", outputCol="white_rating_index",stringOrderType="alphabetAsc", handleInvalid = 'keep')
    indexer_3 = StringIndexer(inputCol="black_rating", outputCol="black_rating_index",stringOrderType="alphabetAsc", handleInvalid = 'keep')

    # vector assembler is used to have features in a single vector
    asb = VectorAssembler(inputCols=FEATURES, outputCol='features')

    # create a logistic-regression model for classification
    lr = LogisticRegression(labelCol="winner_index", featuresCol="features", maxIter=100, regParam=0.3, elasticNetParam=0.8)
    
    # we need ovr because we have 3 labels
    ovr = OneVsRest(featuresCol= 'features', labelCol= 'winner_index', classifier=lr)

    # create the pipeline
    pipeline = Pipeline(stages= [indexer_1,indexer_2,indexer_3, asb, ovr])

    return pipeline

# training and evaluation of the model
def train_and_evaluate(df, pipeline):

    # split dataset
    (training_data, test_data) = df.randomSplit([ .8, .2 ], seed= 1001)

    # fit the model with training data and make predictions
    pipeline_model = pipeline.fit(training_data)
    predictions = pipeline_model.transform(test_data)

    # create an evaluator to compute accuracy on predictions
    evaluator = MulticlassClassificationEvaluator(
        labelCol='winner_index', 
        predictionCol='prediction', 
        metricName='accuracy'
    )

    accuracy = evaluator.evaluate(predictions)
    print('Test accuracy = ', accuracy)

    return pipeline_model

# needed to make data enrichment (using the model to make prediction) to each batch 
def for_each_batch(df,epoch_id):

    # selecting coloumn of interest
    trasformed_df = df.select(col("id"),col("speed"),col("white"),col("black"),col("white_rating"),col("black_rating"))

    # applying the model to the dataframe
    trasformed_df = my_model.transform(trasformed_df)

    # bringing back the index to to the original label
    ind_str = IndexToString(inputCol='prediction', outputCol='prediction_1', labels=['white', 'black', 'draw'])
    trasformed_df = ind_str.transform(trasformed_df)

    print("sending data to elasticsearch")

    trasformed_df = trasformed_df.select(col("id"),col("speed"),col("white"),col("black"),col("white_rating"),col("black_rating"),col("prediction"))
    write_to_es(trasformed_df)

# setting up spark
sparkConf = SparkConf().set("es.nodes", "elasticsearch") \
                        .set("es.port", "9200")
sc = SparkContext(appName="live_chess_2", conf=sparkConf)
spark = SparkSession(sc)
sc.setLogLevel("ERROR")


# loading the dataset for the model training 
dataset = spark.read.option("header",True).csv("training_games_4.csv")

es = Elasticsearch(hosts=elastic_host)

# creating and evaluating the pipeline
my_pipeline = create_pipeline(dataset)
my_model = train_and_evaluate(dataset,my_pipeline)

# reads the data from kafka
df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafkaServer) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .selectExpr("data.*")

# writing to elasticsearch
df.writeStream.foreachBatch(for_each_batch).start(elastic_index).awaitTermination()