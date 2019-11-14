package com.jiangbaoqiao.spark.RDDTest

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object ScalaWordCountStreaming {

  def main(args: Array[String]): Unit = {

    // 创建SparkConf 并且 设置配置参数
    val conf = new SparkConf().setAppName("ScalaWordCountStreaming").setMaster("local")
    println("++++++++++++++++++++111111111111111111111111111111111111111111111")

    // 几秒执行一次
    val ssc =new StreamingContext(conf,Seconds(5))

    ssc.sparkContext.setLogLevel("WARN")

    ssc.checkpoint("D:\\big_data\\Program\\spark_test\\src\\main\\scala\\com\\jiangbaoqiao\\spark\\RDDTest\\ckSM")

    val dStream =ssc.socketTextStream("172.20.10.39 ",8888)

    val tuples: DStream[(String,Int)] =dStream.flatMap(_.split(" ")).map((_,1))



    println("tuples:++++++++++++++++++++: " +tuples)
    tuples.print(5)

    // 按批次累计需要调用updateStateBykey
    val res :DStream[(String,Int)] = tuples.updateStateByKey(func,new HashPartitioner(ssc.sparkContext.defaultParallelism),false)

    println("_____________________")
    res.print(5)

    ssc.start()

    ssc.awaitTermination()

//
//    // sc 是SparkContext ，他是spark 程序执行的入口
//    val sc = new SparkContext(conf)
//
//    sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).saveAsTextFile(args(1))
//
//    sc.stop()
  }
   val func =(it :Iterator[(String,Seq[Int],Option[Int])]) =>{
        it.map( t=>{
          (t._1,t._2.sum +t._3.getOrElse(0))
        })
   }

}
