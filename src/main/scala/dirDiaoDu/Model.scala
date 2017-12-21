package dirDiaoDu

import scala.collection.mutable

/**
  * Created by Administrator on 2017/12/20.
  */
object Model {
  //多个上层目录
  val dir = Array("hdfs://192.4.11.1/ExactQueryWatchDir")
  //达到该值。进入最高峰
  val upperDirSizeInGB =10
  //进入最高峰时 运行的线程数
  val upperThreadLimit = 10
  //最高峰 最大轮询次数
  val maxRetires = 10
  //低峰 运行线程数
  val lowerThreadLimit = 5
  //低峰 最大轮询次数
  val minRetires = 3
  //打到该值，进入最低峰
  val lowerDirSizeInMB = 100
  //最低峰 最大轮询次数
  val lowerRetires = 1
  //文件数量下限
  val minFileListLength = 10
  //文件大小下限
  val minFileListSizeInMB = 10
  //当前处理目录对应的下标值
  var currentDataTypeIndex = 0
  //每种类型等待次数
  val dirsAndWaitTimeMap = new mutable.HashMap[String,Int]()
  //用于判断是否第一次运行
  var isFirstTimeRunning = 1

}
