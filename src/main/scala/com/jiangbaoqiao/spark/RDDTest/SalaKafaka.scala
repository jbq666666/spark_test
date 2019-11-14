package com.jiangbaoqiao.spark.RDDTest

import com.jiangbaoqiao.spark.RDDTest.ScalaWordCountStreaming.func
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object SalaKafaka {



  val func =(it :Iterator[(String,Seq[Int],Option[Int])]) =>{
    it.map( t=>{
      (t._1,t._2.sum +t._3.getOrElse(0))
    })
  }



  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName("kafkaSMWC").getOrCreate()

    val conf: SparkConf = new SparkConf().setAppName("kafkaWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Milliseconds(50000))

    ssc.sparkContext.setLogLevel("WARN")

    ssc.checkpoint("D:\\big_data\\Program\\spark_test\\src\\main\\scala\\com\\jiangbaoqiao\\spark\\RDDTest\\ckSM")


    val map = Map("test" -> 1)
    val createStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, "localhost:2181", "groupId", map, StorageLevel.MEMORY_AND_DISK_SER)
    val lines: DStream[String] = createStream.map(_._2)

    lines.print(5)


    lines.flatMap(_.split(" ")).map((_,1)).updateStateByKey(func,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)



    ssc.start()

    ssc.awaitTermination()






  }

}
