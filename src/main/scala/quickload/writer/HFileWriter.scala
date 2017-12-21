package quickload.writer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.hbase.client.Connection
import org.apache.log4j.Logger
import quickload.writer.WriteHFile.WriterLength
import java.util

import org.apache.hadoop.hbase.spark.BulkLoadPartitioner
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

/**
  * wl的使用原则
  * 1、不同的tableName不同的wl
  * 2、不同的region使用不同的wl
  * 3、数据首先按照字典排序，所以一个tableName只记录一个wl
  * 当发现即将写入的数据跨越了region的时候，则将往前的wl关闭
  *
  * Created by Administrator on 2017/11/28.
  */
class HFileWriter {
  val log : Logger = Logger.getLogger(classOf[HFileWriter])
  def writeToHFile(dataPartion:List[(String,Array[Byte],Array[Byte],Array[Byte],Array[Byte])],
                  conn : Connection,
                  fs: FileSystem,
                  hbaseConf :Configuration,
                  rootDir: String): Unit ={
    var writerMapping = new mutable.HashMap[String /*tableNAme*/,(Array[Byte]/*rowKey*/,WriterLength)]()

    //获取该RDD的所有tableName
    val tableNameSet = dataPartion.map(_._1).toSet

    //获取每个表中各个region的startKeys
    val tableStartKeysMapping = tableNameSet.map(tableName =>{
      (tableName -> conn.getRegionLocator(TableName.valueOf(tableName)).getStartKeys)
    }).toMap

    //执行数据循环写入
    dataPartion.foreach(data =>{
      val tableName = data._1
      val rowKey = data._2
      val family = data._3
      val qual = data._4
      val value = data._5

      writerMapping.get(tableName) match {
        case Some(s) =>{
          log.info("-----------------use wl")

          val lastRowKey = s._1
          val lastRowKeyStr = lastRowKey.toString
          val wl = s._2

          val startKeys = tableStartKeysMapping(tableName)
          val startKeysStr = startKeys.map(x=>Bytes.toString(x))

          val lastRowKeyIndex = util.Arrays.binarySearch(startKeysStr,lastRowKeyStr,WriteHFile.comparator)
          val curRowKeyIndex = util.Arrays.binarySearch(startKeysStr,lastRowKeyStr,WriteHFile.comparator)

          //如果index是一样的，则可以继续使用该wl进行写入
          if(lastRowKeyIndex == curRowKeyIndex){
            val kv = new KeyValue(rowKey,family,qual,System.currentTimeMillis(),value)
            wl.writer.append(kv)
            wl.written += kv.getLength
            //更新rowKey信息
            writerMapping += (tableName ->(rowKey,wl))
          }
          //如果不一样则需要关闭当前wl，重新 new一个
          else{
            log.info("-------------------close wl and new wl")
            //关闭当前wl
            WriteHFile.rollWriter(fs , wl , new BulkLoadPartitioner(startKeys),lastRowKey,false)

            //新建一个wl
            val newWl = WriteHFile.writeValueToHFile(
              rowKey,family,qual,value,System.currentTimeMillis(),
              fs,conn,TableName.valueOf(tableName),hbaseConf,
              WriteHFile.getDefaultFamilyOptions(),
              rootDir+tableName.split(":",-1).mkString("/"))
            //更新
            writerMapping += (tableName -> (rowKey,newWl))
          }
        }
          //如果没有则需要新建
        case None =>{
          log.info("----------new wl")

          val wl = WriteHFile.writeValueToHFile(
            rowKey,family,qual,value,System.currentTimeMillis(),
            fs,conn,TableName.valueOf(tableName),hbaseConf,
            WriteHFile.getDefaultFamilyOptions(),
            rootDir+tableName.split(":",-1).mkString("/"))

          //更新
          writerMapping += (tableName ->(rowKey,wl))
        }
      }
    })

    //关闭所有没有关闭的wl
    writerMapping.foreach(kv =>{
      log.info("--------close wl")
      val tableName = kv._1
      val startKeys = tableStartKeysMapping(tableName)
      val rowKey = kv._2._1
      val wl = kv._2._2
      //关闭当前的wl
      WriteHFile.rollWriter(fs,wl,new BulkLoadPartitioner(startKeys),rowKey,false)
    })
  }
}
