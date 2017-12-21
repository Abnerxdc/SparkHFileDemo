package quickload.writer

import java.io.IOException
import java.net.InetSocketAddress
import java.util.{Comparator, UUID}

import org.apache.hadoop.conf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.KeyValue.KVComparator
import org.apache.hadoop.hbase.{HConstants, HRegionLocation, KeyValue, TableName}
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.fs.HFileSystem
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.{DataBlockEncoder, DataBlockEncoding}
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, HFile, HFileContextBuilder}
import org.apache.hadoop.hbase.regionserver.{BloomType, HStore, StoreFile}
import org.apache.hadoop.hbase.spark.{BulkLoadPartitioner, FamilyHFileWriteOptions}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger

/**
  * Created by Administrator on 2017/11/28.
  */
class WriteHFile {

}
object WriteHFile{
  var log = Logger.getLogger(classOf[WriteHFile])

  val comparator:Comparator[String] = new Comparator[String] {
    override def compare(o1: String, o2: String): Int = {
      Bytes.compareTo(Bytes.toBytes(o1),Bytes.toBytes(o2))
    }
  }

  def getDefaultFamilyOptions() : FamilyHFileWriteOptions = {
    return new FamilyHFileWriteOptions(Compression.Algorithm.GZ.toString,BloomType.NONE.toString,HConstants.DEFAULT_BLOCKSIZE,DataBlockEncoding.NONE.toString)
  }
  def getOtherFamilyOptions() : FamilyHFileWriteOptions = {
    return new FamilyHFileWriteOptions(Compression.Algorithm.GZ.toString,BloomType.NONE.toString,HConstants.DEFAULT_BLOCKSIZE,DataBlockEncoding.NONE.toString)
  }

  class WriterLength(var written : Long, val writer: StoreFile.Writer)

  def rollWriter(fs: FileSystem,
                 writerLength: WriterLength,
                 regionSplitPartition: BulkLoadPartitioner,
                 previousRow: Array[Byte],
                 compactionExclude : Boolean): Unit ={
    if(writerLength != null && writerLength.writer != null){
      log.debug("Write = "+writerLength.writer.getPath+
        (if (writerLength.written == 0) "" else ", wrote = "+ writerLength.written))
        closeHFileWriter(fs,writerLength.writer,regionSplitPartition,previousRow,compactionExclude)
    }
  }
  def closeHFileWriter(fs :FileSystem,
                       w: StoreFile.Writer,
                       regionSplitPartitioner: BulkLoadPartitioner,
                       previousRow: Array[Byte],
                       compactionExclude : Boolean): Unit ={
    if(w != null){
      w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
      Bytes.toBytes(System.currentTimeMillis()))
      w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
        Bytes.toBytes(regionSplitPartitioner.getPartition(previousRow)))
      w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
        Bytes.toBytes(true))
      w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
        Bytes.toBytes(compactionExclude))
      w.appendTrackedTimestampsToMetadata()
      w.close()

      val srcPath = w.getPath
      // In the new path you will see that we are using substring. This is to
      // remove the '_' character in front of the HFile name. '_' is a character
      // that will tell HBase that this file shouldn't be included in the bulk load
      // tThis feature is to protect for unfinished HFiles being submitted ti HBase
      val newPath = new Path(w.getPath.getParent,w.getPath.getName.substring(1))
      if(!fs.rename(srcPath,newPath)){
        throw new IOException("Unable to rename '"+ srcPath + "' to "+newPath)
      }
    }
  }
  def writeValueToHFile(rowKey: Array[Byte],
                        family: Array[Byte],
                        qualifier: Array[Byte],
                        cellValue: Array[Byte],
                       nowTimeStamp: Long,
                       fs: FileSystem,
                       conn: Connection,
                       tableName: TableName,
                       conf: Configuration,
                       familyOptions: FamilyHFileWriteOptions,
                       stagingDir: String
                       ):WriterLength = {
    val wl = {
      val familyDir = new Path(stagingDir,Bytes.toString(family))
      fs.mkdirs(familyDir)

      val loc: HRegionLocation = {
        try{
          val locator = conn.getRegionLocator(tableName)
          locator.getRegionLocation(rowKey)
        }catch {
          case e: Throwable =>
          log.warn("there's something wrong when locating rowKey : "+Bytes.toString(rowKey))
          null
        }
      }
      if(null == loc){
        if(log.isTraceEnabled){
          log.warn("failed to get region location , so use default writer: "+Bytes.toString(rowKey))
        }
        getNewHFileWriter(family = family,
        conf = conf,
        favoredNodes = null,
        fs = fs,
        familyDir = familyDir,
        familyOptions)
      }
      else {
        if(log.isDebugEnabled){
          log.debug("first rowKey : ["+Bytes.toString(rowKey)+"]")
        }
        val initialIsa = new InetSocketAddress(loc.getHostname,loc.getPort)
        if(initialIsa.isUnresolved){
          if(log.isTraceEnabled){
            log.trace("failed to resolve bind address: "+loc.getHostname+":"+loc.getPort+", so use default writer")
          }
          getNewHFileWriter(family,
          conf,
          null,
          fs,
          familyDir,
          familyOptions)
        }
        else {
          if(log.isDebugEnabled){
            log.debug("use favored nodes writer: "+initialIsa.getHostString)
          }
          getNewHFileWriter(family,
          conf,Array[InetSocketAddress](initialIsa),
          fs,
          familyDir,familyOptions)
        }
      }
    }
    val keyValue = new KeyValue(rowKey,
    family,
    qualifier,nowTimeStamp,cellValue)

    wl.writer.append(keyValue)
    wl.written +=keyValue.getLength
    wl
  }

  def getNewHFileWriter(family: Array[Byte],conf: Configuration,
                       favoredNodes: Array[InetSocketAddress],
                       fs: FileSystem,
                       familyDir: Path,
                       familyOptions: FamilyHFileWriteOptions):WriterLength = {
    val tempConf = new Configuration(conf)
    tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f)
    val contextBulider = new HFileContextBuilder()
    .withCompression(Algorithm.valueOf(familyOptions.compression))
    .withChecksumType(HStore.getChecksumType(conf))
    .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
    .withBlockSize(familyOptions.blockSize)
    .withDataBlockEncoding(DataBlockEncoding.valueOf(familyOptions.dataBlockEncoding))

    if (HFile.getFormatVersion(conf) >= HFile.MIN_FORMAT_VERSION_WITH_TAGS){
      contextBulider.withIncludesTags(true)
    }

    val hFileContext = contextBulider.build()

    val writer = new StoreFile.WriterBuilder(conf,new CacheConfig(tempConf),new HFileSystem(fs))
    .withBloomType(BloomType.valueOf(familyOptions.bloomType))
    .withComparator(new KVComparator)
    .withFileContext(hFileContext)
    .withFilePath(new Path(familyDir,"_"+UUID.randomUUID().toString.replaceAll("-","")))
    .withFavoredNodes(favoredNodes)
    .build()
    new WriterLength(0,writer)
  }

  def rollWriters(fs: FileSystem,
                  writerMap: scala.collection.mutable.HashMap[String,WriterLength],
                  regionSplitPartitioner: BulkLoadPartitioner,
                  previousRow: Array[Byte],
                  compactionExclude: Boolean): Unit ={
    writerMap.values.foreach(wl =>{
      if(wl.writer != null){
        log.info("Writer="+wl.writer.getPath+(if(wl.written == 0) "" else ", wrote="+wl.written))
        closeHFileWriter(fs,wl.writer,regionSplitPartitioner,previousRow,compactionExclude)
      }
    })
    writerMap.clear()
  }

}
