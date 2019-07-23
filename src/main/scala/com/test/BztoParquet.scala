package com.test

import com.util.{Log2Parquet, StringUtil}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 反射的方式
  */
object BztoParquet {
  def main(args: Array[String]): Unit = {
    // 先是判断目录是否为空
    if(args.length != 2){
      sys.exit()
    }
    val Array(inputPath,outputPath) = args

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置spark的持久化机制
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 读取文件
    val lines = sc.textFile(inputPath)
    val logsRDD = lines.map(t=>t.split(",")).filter(_.length >= 85).map(arr=>
      // 调用util中的Log2Parqurt 将log日志信息转化成Parquet形式
      new Log2Parquet(arr(0),
        StringUtil.StringtoInt(arr(1)),
        StringUtil.StringtoInt(arr(2)),
        StringUtil.StringtoInt(arr(3)),
        StringUtil.StringtoInt(arr(4)),
        arr(5),
        arr(6),
        StringUtil.StringtoInt(arr(7)),
        StringUtil.StringtoInt(arr(8)),
        StringUtil.StringtoDouble(arr(9)),
        StringUtil.StringtoDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        StringUtil.StringtoInt(arr(17)),
        arr(18),
        arr(19),
        StringUtil.StringtoInt(arr(20)),
        StringUtil.StringtoInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        StringUtil.StringtoInt(arr(26)),
        arr(27),
        StringUtil.StringtoInt(arr(28)),
        arr(29),
        StringUtil.StringtoInt(arr(30)),
        StringUtil.StringtoInt(arr(31)),
        StringUtil.StringtoInt(arr(32)),
        arr(33),
        StringUtil.StringtoInt(arr(34)),
        StringUtil.StringtoInt(arr(35)),
        StringUtil.StringtoInt(arr(36)),
        arr(37),
        StringUtil.StringtoInt(arr(38)),
        StringUtil.StringtoInt(arr(39)),
        StringUtil.StringtoDouble(arr(40)),
        StringUtil.StringtoDouble(arr(41)),
        StringUtil.StringtoInt(arr(42)),
        arr(43),
        StringUtil.StringtoDouble(arr(44)),
        StringUtil.StringtoDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        StringUtil.StringtoInt(arr(57)),
        StringUtil.StringtoDouble(arr(58)),
        StringUtil.StringtoInt(arr(59)),
        StringUtil.StringtoInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        StringUtil.StringtoInt(arr(73)),
        StringUtil.StringtoDouble(arr(74)),
        StringUtil.StringtoDouble(arr(75)),
        StringUtil.StringtoDouble(arr(76)),
        StringUtil.StringtoDouble(arr(77)),
        StringUtil.StringtoDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        StringUtil.StringtoInt(arr(84)))
    )
      val df = sQLContext.createDataFrame(logsRDD)
    df.write.parquet(outputPath)
    sc.stop()
  }
}
