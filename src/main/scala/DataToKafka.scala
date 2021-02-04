import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming.ConsumerSocketSource
import org.apache.spark.sql.functions.{col, explode, split, struct, to_json}
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

object DataToKafka {

  /**
   *
   * @param sendcode 根据发送请求获取不通数据类型
   * @return true 代表请求float  false 代表请求 boolean
   */
 def flagData(sendcode:String):Boolean={
    if(sendcode.startsWith("3E2A07D1"))
      true
    else if(sendcode.startsWith("3E2A07D2"))
      false
    else
      throw new Exception(s"输入的$sendcode 不合法")
  }
  def main(args: Array[String]): Unit ={
    val session: SparkSession = SparkSession.builder().appName("dataToKafka").master("local[3]").getOrCreate()
    val sendcode: String ="3E2A07D10000006F"
    val datatype=flagData(sendcode)
    val source: DataFrame = session
      .readStream
      .format("org.apache.spark.sql.execution.streaming.ConsumerSocketSourceProvider")
      .option("host","172.17.3.178")
      .option("port","5002")
      .option("sendCode",sendcode)
      .option("dataType",datatype)
      .option("proppath","E:\\aa.txt")
      .option("includeTimestamp",true)
      .load()

    source.isStreaming
    source.printSchema()
   // val tagName: DataFrame = session.read.textFile("E:\\aa.txt").select(functions.decode(col("value"), "gbk").as("value"))

   // session.conf.set("spark.sql.streaming.checkpointLocation","hdfs://172.17.3.194:8020/tmp/checkpoint1")
   /* val query: StreamingQuery = source.writeStream
      .queryName("aggregates")    // this query name will be the table name
      .outputMode("append")
      .format("memory")
      .start()*/


    //session.sql("select * from aggregates").show()
 //    query.awaitTermination()

    val query: StreamingQuery = source.select(to_json(struct(col("timestamp"),col("tagName"),col("tagValue"))).alias("value")).writeStream.format("kafka").option("kafka.bootstrap.servers", "172.17.3.194:6667").option("checkpointLocation","hdfs://172.17.3.194:8020/tmp/checkpoint2")
      .option("topic", "tcp").start()

    query.awaitTermination()

  }

}
