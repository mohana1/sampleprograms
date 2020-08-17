package org.inceptez.streaming
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.elasticsearch.spark._ 
import org.elasticsearch.spark.sql._; 
object kafkastreamjsones {
def main(args:Array[String])
{
val sparkConf = new SparkConf().setAppName("kafkastream").setMaster("local[*]")
val sparkcontext = new SparkContext(sparkConf)
        sparkConf.set("es.nodes", "localhost")
       sparkConf.set("es.port", "9200")
       sparkConf.set("es.index.auto.create", "true");
       sparkConf.set("es.mapping.id","rnum");
sparkcontext.setLogLevel("ERROR")
val sqlcontext=new org.apache.spark.sql.SQLContext(sparkcontext)
val ssc = new StreamingContext(sparkcontext, Seconds(10))
//ssc.checkpoint("checkpointdir")

val kafkaParams = Map[String, Object](
"bootstrap.servers" -> "localhost:9092",
"key.deserializer" -> classOf[StringDeserializer],
"value.deserializer" -> classOf[StringDeserializer],
"group.id" -> "we33",
"auto.offset.reset" -> "latest"
)
val topics = Array("tk1")

val stream = KafkaUtils.createDirectStream[String, String](ssc,
PreferConsistent, // PreferBroker / PreferFixed
Subscribe[String, String](topics, kafkaParams))
val kafkastream = stream.map(record => (record.key, record.value))
val inputStream = kafkastream.map(rec => rec._2);
inputStream.foreachRDD{x=>
         println("checking rdd is not empty")
            if(!x.isEmpty())
            {  
              println("iterating rdd")
               val df = sqlcontext.read.option("multiLine", true).option("mode", "PERMISSIVE").json(x)
               df.createOrReplaceTempView("jsondata")
               val dftoes=sqlcontext.sql("select row_number() over(partition by id order by type) as rnum, id,type,name,explode(links) from jsondata")
               dftoes.saveToEs("jsondata/food")    
      println("data writtern into es index jsondata/food")
               df.printSchema()
    //x.foreach(println)
    
    }
}
ssc.start()
ssc.awaitTerminationOrTimeout(300000)
}}












