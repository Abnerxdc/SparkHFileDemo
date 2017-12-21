package quickload.core

import java.io.FileInputStream
import java.util.Properties
import scala.collection.JavaConversions._

import quickload.util.DataUtil

/**
  * Created by Administrator on 2017/7/14.
  */
object ConfigManager {
  var mConfPath = ""
  var mPath = ""
  val mProp : Properties = new Properties()


  def setConfPath(iConfPath: String): Unit ={
    mPath = iConfPath+"/application.properties"
    getProp(mPath)
  }

  private def getProp(mConfPath: String) ={
    mProp.load(new FileInputStream(mConfPath))
  }

  def getSparkConf():Map[String ,String] = {
    DataUtil.getPropsByPrefix(mProp,"sparkconf.").toMap
  }

  def getInputFilePath():String = {
    val filePath : String = mProp.getProperty("InputFilePath")
    filePath
  }

  def getHbaseConf():Map[String,String]={
    DataUtil.getPropsByPrefix(mProp,"hbaseconf.").toMap
  }

  def getHdfsPath():String = {
    mProp.getProperty("hdfsPath")
  }
  def getStoreFileRootPath():String = {
    mProp.getProperty("storeFileRootPath")
  }

}
