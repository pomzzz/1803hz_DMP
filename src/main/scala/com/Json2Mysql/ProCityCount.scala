package com.Json2Mysql

import java.util.Properties

import com.constant.Constant
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 3.2 统计各省市数据量的分布情况
  */
object ProCityCount {
  def main(args: Array[String]): Unit = {
    // 判断目录中参数是否正确
    if (args.length != 2) {
      println("请输入正确的参数")
      sys.exit()
    }

    // 当目录参数正确时，创建储存输入输出目录的数组
    val Array(inputPath,outputPath) = args

    // 创建spark的执行入口
    val conf = new SparkConf().setAppName(Constant.SPARK_APP_NAME_ProCityCount).setMaster(Constant.SPARK_LOCAL)
    // 设置序列化机制
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    // 设置spark sql的压缩方式，注意spark1.6版本以后不是snappy，到2.2以后是默认的压缩方式
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")

    // 读取parquet文件
    val df = sQLContext.read.parquet(inputPath)


    /**
      * spark core
      */
      // 将DataFrame转成row  按照省份和城市为key，1位value
    val rdd = df.rdd.map(t=>((t.getAs[String]("provincename"),t.getAs[String]("cityname")),1))
    // 聚合操作
    val reduce = rdd.reduceByKey(_+_)
    reduce.foreach(println)

    /**
      * spark sql
      */

//    // 注册临时表
//    df.registerTempTable("ProCity")
//    // 执行sql语句
//    val results = sQLContext.sql("select provincename,cityname,count(*) from ProCity group by provincename,cityname")
//    // 将结果保存到本地路径中
//    // 分区5个
//    results.coalesce(5).write.json(outputPath)
//
//    // 将结果保存到mysql中
//    val load = ConfigFactory.load()
//    val prop = new Properties()
//    prop.setProperty("user",load.getString("jdbc.user"))
//    prop.setProperty("password",load.getString("jdbc.password"))
//    // append 附加，往后面加值 跟overwrite相反
//    // url，表名，prop
//    results.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),"procity",prop)

//    // 第二种，需要在mysql中建表
//    val prop = new Properties()
//    try{
//      // 读取配置文件的路径
//      val in_basic = ProCityCount.getClass.getClassLoader.getResourceAsStream("basic.properties")
//      prop.load(in_basic)
//
//      prop.put("user",prop.getProperty("jdbc.user"))
//      prop.put("password",prop.getProperty("jdbc.password"))
//      prop.put("driver",prop.getProperty("jdbc.driver"))
//      val url = prop.getProperty("jdbc.url")
//      results.write.mode(SaveMode.Append).jdbc(url,"procity",prop)
//    }catch{
//      case e: Exception => e.printStackTrace()
//    }



  // 关闭sparkContext
    sc.stop()
  }
}
