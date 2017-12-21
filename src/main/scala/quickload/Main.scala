package quickload

import java.io.File

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import quickload.core.{AppInit, ConfigManager, SparkManager}
import quickload.writer.HFileWriter

import scala.io.Source

/**
  * Created by Administrator on 2017/11/28.
  */
object Main {
  def main(args: Array[String]): Unit = {
    AppInit.initApp()
    val sc = SparkManager.sparkClusterContextInstance(ConfigManager.getSparkConf())

    val file = new File(ConfigManager.getInputFilePath())
    try{
      val hFileList = scala.collection.mutable.Set[String]()
      val dataList = Source.fromFile(file).getLines().map(line =>{
        val lineList = line.split(",")
        hFileList += lineList(0)
        (lineList(0),Bytes.toBytes(lineList(1)),Bytes.toBytes(lineList(2)),Bytes.toBytes(lineList(3)),Bytes.toBytes(lineList(4)))
      })

      val dataRdd = sc.makeRDD(dataList.toSeq)
      val hbaseConfMapBc = sc.broadcast(ConfigManager.getHbaseConf())
      val hdfsPathBc = sc.broadcast(ConfigManager.getHdfsPath())
      val storeFileRootPathBc = sc.broadcast(ConfigManager.getStoreFileRootPath())

      dataRdd.foreachPartition(dataPartition =>{
        val dataPartitionList = dataPartition.toList

        //分部式环境
        val hbaseConf = HBaseConfiguration.create()
        hbaseConfMapBc.value.map(kv =>{
          hbaseConf.set(kv._1 , kv._2)
        })
        val hdfsPath = hdfsPathBc.value
        val storeFileRootPath = storeFileRootPathBc.value

        //每个分区维护各自的链接
        val conn : Connection = ConnectionFactory.createConnection(hbaseConf)
        val fs : FileSystem = new Path(hdfsPath).getFileSystem(hbaseConf)

        val writer = new HFileWriter()
        writer.writeToHFile(dataPartitionList, conn , fs , hbaseConf , storeFileRootPath+"/")
      })
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }
}
