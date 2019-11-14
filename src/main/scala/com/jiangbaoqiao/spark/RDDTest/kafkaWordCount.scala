package com.jiangbaoqiao.spark.RDDTest

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object kafkaWordCount {

  val updateFunc=(iterator: Iterator[(String, Seq[Int], Option[Int])])=>{
    iterator.map( t=> {
      (t._1,t._2.sum + t._3.getOrElse(0))
    })
  }

  /**
    * Created by lq on 2017/8/29.
    */
  object JedisConnectionPool {

    val config = new JedisPoolConfig()
    //最大连接数
    config.setMaxTotal(10)
    //最大空闲连接数
    config.setMaxIdle(5)
    //当调用borrow object 方法时,是否进行有效性验证
    config.setTestOnBorrow(true)
    val pool = new JedisPool(config, "localhost", 6379)

    def getContion(): Jedis = {
      pool.getResource
    }
  }

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels()

    val conf: SparkConf = new SparkConf().setAppName("kafkaWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Milliseconds(5000))

//    ssc.checkpoint("D:\\big_data\\Program\\spark_test\\src\\main\\scala\\com\\jiangbaoqiao\\spark\\RDDTest\\ckSM")
    ssc.checkpoint("hdfs://localhost:9000/datas/spark/CK")
    ssc.sparkContext.setLogLevel("WARN")

    val zkQuorum = "localhost:2181"
    val groupID = "g1"
    val topic = Map("test" -> 2)

    // 从 kafka 中拉取  KV 消息，K为指定topic ，v为消息 值
    val topicAndLine: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupID, topic, StorageLevel.MEMORY_ONLY)
    // 获取Kafka中  的一行内容
    val lines: DStream[String] = topicAndLine.map(_._2)

    // 数据加工
    //    val reduced: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    // 数据加工，结果是可以update 的场景
    val reduced: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc ,new HashPartitioner(ssc.sparkContext.defaultMinPartitions), true)

    // 结果处理,打印；存redis，存hdfs，存orc，存parquet。。。
    reduced.print()
    reduced.foreachRDD( rdd => {
      // 对拿到的 每一个 RDD 结果，将结果写入 Redis
      // new  一个Redis 连接池
      // 伪代码

      rdd.foreachPartition( part =>{
        val jedis = JedisConnectionPool.getContion()
        part.foreach( t=> {
          jedis.set(t._1,t._2.toString)
        })

         jedis.close()
      })

    })

    ssc.start()
    ssc.awaitTermination()

  }
}
