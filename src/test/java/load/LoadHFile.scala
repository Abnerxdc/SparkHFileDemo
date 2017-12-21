package load

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable}
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import quickload.core.AppInit

/**
  * Created by Administrator on 2017/12/6.
  */
object LoadHFile {
  def main(args: Array[String]): Unit = {
    AppInit.initApp()
    val tableName = "AbnerTestTable"

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM,"192.168.1.106")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hbaseConf.set("hbase.master","192.168.1.106")
    hbaseConf.set("hbase.zookeeper.master","192.168.1.106")
    val conn = ConnectionFactory.createConnection(hbaseConf)

    val hbaseTableName = TableName.valueOf(tableName)
    val table = conn.getTable(hbaseTableName)
    val load = new LoadIncrementalHFiles(hbaseConf)
    load.doBulkLoad(new Path("hdfs://192.168.1.106:8020/user/Abner/AbnerTestTable"),table.asInstanceOf[HTable])
  }
}
