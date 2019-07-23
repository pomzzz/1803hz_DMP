package com.app

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 地区指标
  */
object LocationCount {
  def main(args: Array[String]): Unit = {
    // 判断目录参数是否为空
    if (args.length != 2){
      println("目录参数有误")
      sys.exit()
    }
    // 目录参数正确时，创建array数组存储输入和输出的目录路径
    val Array(inputPath,outputPath) = args
    // 创建spark的执行入口
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化机制
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    // 设置spark sql的压缩方式，注意spark1.6版本以后不是snappy，到2.2以后是默认的压缩方式
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")

    // spark core 实现
    // 读取文件的路径
    val df = sQLContext.read.parquet(inputPath)
    val baseData = df.map(row=>{
      // 去值key
      val pro = row.getAs[String]("provincename")  // 省份
      val city = row.getAs[String]("cityname")  // 城市
      val ispid = row.getAs[Int]("ispid") // 运营商id
      val ispname = row.getAs[String]("ispname") // 运营商名称
      // 取值value
      val requestmode = row.getAs[Int]("requestmode")  // 数据请求方式（1:请求、2:展示、3:点击）
      val processnode = row.getAs[Int]("processnode")  // 流程节点（1：请求量 kpi 2：有效请求 3：广告请求

      val iseffective = row.getAs[Int]("iseffective")  // 有效标识（有效指可以正常计费的）(0：无效 1：有效
      val isbilling = row.getAs[Int]("isbilling")  // 是否收费（0：未收费 1：已收费）
      val isbid = row.getAs[Int]("isbid")  // 是否 rtb
      val iswin = row.getAs[Int]("iswin")  // 是否竞价成功
      val adorderid = row.getAs[Int]("adorderid")  // 广告 id
      val winprice = row.getAs[Double]("winprice")  // rtb 竞价成功价格
      val adpayment = row.getAs[Double]("adpayment")  // 转换后的广告消费

      // 处理数据 ,调用业务方法
      //处理原始，有效，广告的请求方法
      val reqList = AppUtils.Request(requestmode,processnode)
      // 处理竞价数，竞价成功数，广告成品，广告消费
      val adList = AppUtils.appad(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
      // 处理点击量和展示量
      val clickList = AppUtils.ReCount(requestmode,iseffective)
      //处理数据
      ((pro,city),  // 省份城市
        reqList++adList++clickList,
        (ispid,ispname) // 运营商ID 名称
       )
    })
    //进行聚合集合的数据操作
    /**
      * 3.2.1地域分布
      */
    val result1 = baseData.map(t=>(t._1,t._2)).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => {
      t._1._1 + "," + t._1._2 + "," + t._2.mkString(",")
    })
//      .foreach(println)
    //
//    val load = ConfigFactory.load()
//    val prop = new Properties()
//    prop.setProperty("user",load.getString("jdbc.user"))
//    prop.setProperty("password",load.getString("jdbc.password"))
    // 保存路径
//    result1.coalesce(5).saveAsTextFile(outputPath)

    /**
      * 3.2.2终端设备
      */
    val result2 = baseData.map(t=>(t._3,t._2)).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => {
      t._1._1 + "," + t._1._2 + "," + t._2.mkString(",")
    })//.foreach(println)
        .coalesce(1).saveAsTextFile(outputPath)
    // 关闭 sparkContest
    sc.stop()
  }
}
