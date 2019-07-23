package com.util

/**
  * 类型转化的工具类
  */
object StringUtil {
  // 将字符串转化成整数类型
  def StringtoInt(str: String):Int={
    try{
      str.toInt
    }catch{
      case e :Exception => 0
    }
  }
    // 将字符串转化成小数类型
  def StringtoDouble(str:String):Double={
    try{
      str.toDouble
    }catch{
      case e:Exception => 0.0
    }
  }
}
