package com.test


import com.constant.Constant
import com.util.StringUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Bz2Parquet {
  def main(args: Array[String]): Unit = {
    // 判断目录是否为空
    if(args.length != 2){
      println("目录参数不正确！！！")
      sys.exit()
    }

    // 如果目录的参数正确，创建数组储存输入输出目录
    val Array(inputPath,outputPath) = args

    val conf = new SparkConf().setAppName(Constant.SPARK_APP_NAME_Bz2Parquet).setMaster(Constant.SPARK_LOCAL)
    // 设置序列化机制
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 设置sqlContext转化成parquet的方法
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 读取文件
    val lines = sc.textFile(inputPath)
    // 对读取的数据进行过滤，要求字段大于85个，不然就会组数越界
    // 注意： 当内部的字段为,,,,,,,,时，会无法解析，它会识别为一个元素，
    // 所以我们需要用-1，或是t.length，对处理切割进行处理
    val rowRDD: RDD[Row] = lines.map(t=>t.split(",",-1)).filter(_.length >=85 ).map(arr=>{
      Row(
        arr(0),
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
        StringUtil.StringtoInt(arr(84))
      )
    })
    // 构造 StructType
    val structType = StructType(Seq(
      StructField("sessionid", StringType),
      StructField("advertisersid", IntegerType),
      StructField("adorderid", IntegerType),
      StructField("adcreativeid", IntegerType),
      StructField("adplatformproviderid", IntegerType),
      StructField("sdkversion", StringType),
      StructField("adplatformkey", StringType),
      StructField("putinmodeltype", IntegerType),
      StructField("requestmode", IntegerType),
      StructField("adprice", DoubleType),
      StructField("adppprice", DoubleType),
      StructField("requestdate", StringType),
      StructField("ip", StringType),
      StructField("appid", StringType),
      StructField("appname", StringType),
      StructField("uuid", StringType),
      StructField("device", StringType),
      StructField("client", IntegerType),
      StructField("osversion", StringType),
      StructField("density", StringType),
      StructField("pw", IntegerType),
      StructField("ph", IntegerType),
      StructField("long", StringType),
      StructField("lat", StringType),
      StructField("provincename", StringType),
      StructField("cityname", StringType),
      StructField("ispid", IntegerType),
      StructField("ispname", StringType),
      StructField("networkmannerid", IntegerType),
      StructField("networkmannername", StringType),
      StructField("iseffective", IntegerType),
      StructField("isbilling", IntegerType),
      StructField("adspacetype", IntegerType),
      StructField("adspacetypename", StringType),
      StructField("devicetype", IntegerType),
      StructField("processnode", IntegerType),
      StructField("apptype", IntegerType),
      StructField("district", StringType),
      StructField("paymode", IntegerType),
      StructField("isbid", IntegerType),
      StructField("bidprice", DoubleType),
      StructField("winprice", DoubleType),
      StructField("iswin", IntegerType),
      StructField("cur", StringType),
      StructField("rate", DoubleType),
      StructField("cnywinprice", DoubleType),
      StructField("imei", StringType),
      StructField("mac", StringType),
      StructField("idfa", StringType),
      StructField("openudid", StringType),
      StructField("androidid", StringType),
      StructField("rtbprovince", StringType),
      StructField("rtbcity", StringType),
      StructField("rtbdistrict", StringType),
      StructField("rtbstreet", StringType),
      StructField("storeurl", StringType),
      StructField("realip", StringType),
      StructField("isqualityapp", IntegerType),
      StructField("bidfloor", DoubleType),
      StructField("aw", IntegerType),
      StructField("ah", IntegerType),
      StructField("imeimd5", StringType),
      StructField("macmd5", StringType),
      StructField("idfamd5", StringType),
      StructField("openudidmd5", StringType),
      StructField("androididmd5", StringType),
      StructField("imeisha1", StringType),
      StructField("macsha1", StringType),
      StructField("idfasha1", StringType),
      StructField("openudidsha1", StringType),
      StructField("androididsha1", StringType),
      StructField("uuidunknow", StringType),
      StructField("userid", StringType),
      StructField("iptype", IntegerType),
      StructField("initbidprice", DoubleType),
      StructField("adpayment", DoubleType),
      StructField("agentrate", DoubleType),
      StructField("lomarkrate", DoubleType),
      StructField("adxrate", DoubleType),
      StructField("title", StringType),
      StructField("keywords", StringType),
      StructField("tagid", StringType),
      StructField("callbackdate", StringType),
      StructField("channelid", StringType),
      StructField("mediatype", IntegerType)
    ))

    // 创建DataFrame
    val df = sQLContext.createDataFrame(rowRDD,structType)
    // 保存到指定位置
    // partitionBy 按照什么分区
    df.write.partitionBy("provincename","cityname").parquet(outputPath)
    sc.stop()
  }
}
