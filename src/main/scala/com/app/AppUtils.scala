package com.app

/**
  * 业务方法
  */
object AppUtils {
  // 处理原始，有效，广告的请求方法
  def Request(requestmode:Int,processnode:Int):List[Double]={
    // 第一个参数 代表原始请求
    // 第二个参数 代表有效请求
    // 第三个参数 代表广告请求
    //
    if(requestmode == 1 && processnode == 1){
      List[Double](1,0,0)
    }else if(requestmode == 1 && processnode == 2){
      List[Double](1,1,0)
    }else if(requestmode ==1 && processnode == 3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }

  // 处理竞价数，竞价成功数，广告成品，广告消费
  def appad(iseffective:Int, isbilling:Int, isbid:Int, iswin:Int,
            adorderid:Int, winprice:Double, adpayment:Double):List[Double]={
    // 先判断参与竞价的
    if(iseffective == 1 && isbilling == 1 && isbid == 1){
      // 竞价成功的，
      if(iswin == 1 && adorderid != 0){
        List[Double](1,1,winprice/1000.0,adpayment/1000.0)
      }else{
        // 竞价失败的
        List[Double](1,0,0,0)
      }
    }else{
      // 没有竞价的
      List[Double](0,0,0,0)
    }
  }

  // 处理点击量和展示量
  def ReCount(requestmode:Int,iseffective:Int):List[Double]={
    if (requestmode == 2 && iseffective == 1){
      List[Double](1,0)
    }else if(requestmode == 3 && iseffective == 1){
      List[Double](0,1)
    }else{
      List[Double](0,0)
    }
  }
}
