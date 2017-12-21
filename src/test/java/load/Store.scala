package load


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.regionserver.StoreFile
import quickload.core.AppInit

/**
  * Created by Administrator on 2017/12/11.
  */
object Store {
  def main(args: Array[String]): Unit = {
    AppInit.initApp()
    //分部式环境
    val hbaseConf = HBaseConfiguration.create()

    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM,"192.168.1.106")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hbaseConf.set("hbase.master","192.168.1.106")
    hbaseConf.set("hbase.zookeeper.master","192.168.1.106")

    val fs : FileSystem = new Path("hdfs://192.168.1.106:8020").getFileSystem(hbaseConf)
    println(StoreFile.getUniqueFile(fs,new Path("/hbase")))
  }

}
