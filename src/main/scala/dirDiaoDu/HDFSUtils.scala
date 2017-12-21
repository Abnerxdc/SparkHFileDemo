package dirDiaoDu

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.fs.HFileSystem

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/12/20.
  */
object HDFSUtils {
  def getAllDirSize(pathArray : Array[String]): Int ={
    var size = 0
    pathArray.foreach(x =>{
      if(checkHDFSDirExists(x)){
        size = size+ getPathVolumeForM(x).toInt
      }else{
        println(s"scan dir ${x} does not exist")
      }
    })
    size
  }
  def checkHDFSDirExists(filePath:String): Boolean ={
    val config = new Configuration()
    val hdfs = FileSystem.get(URI.create(filePath),config)
    var isDir = false
    try{
      val path = new Path(filePath)
      val isExits = hdfs.exists(path)
      if(isExits){
        isDir = hdfs.isDirectory(path)
      }
      return isDir
    }catch {
      case e : Exception=>{
        return false
      }
    }finally {
    }
  }
  def getPathVolumeForM(dirPath:String):Long = {
    val config = new Configuration()
    val hdfs = FileSystem.get(URI.create(dirPath),config)
    try{
      val path = new Path(dirPath)
      val size = hdfs.getContentSummary(path).getLength
      return size/(1*1024*1024)
    }catch {
      case e : Exception =>{
        e.printStackTrace()
          return -1
      }
    }finally {

    }
  }
  def listAllFileNames(dir: String):Array[String]={
  var fileNames : Array[String] = null
  val config = new Configuration()
  val hdfs = FileSystem.get(URI.create(dir),config)
    try{
      val path = new Path(dir)
      fileNames = hdfs.listStatus(path).filter(x =>{
        hdfs.isFile(x.getPath())==true
      }).map(_.getPath().toString)
    }catch {
      case e : Exception =>{
        e.printStackTrace()
        println("listAllFileNames Failed ! At dir path : "+dir)
      }
    }
  fileNames
  }

  def getDirsFileSizeFilter(dir:String,filter:String):Long={
    var fileSize = 0.toLong
    val config = new Configuration()
    val hdfs = FileSystem.get(URI.create(dir),config)
    try{
      val path = new Path(dir)
      hdfs.listStatus(path).filter(x =>{
        hdfs.isFile(x.getPath)==true
      }).filter(x => x.toString.endsWith(filter)).foreach(
        x => fileSize += x.getLen
      )
    }catch {
      case e : Exception =>{
        e.printStackTrace()
      }
    }
    fileSize
  }

  def listNFileName2FileSize(dir:String,number:Int):Array[Tuple2[String,Long]]={
    var fileName2FileSizeArray : ArrayBuffer[Tuple2[String,Long]] = ArrayBuffer[Tuple2[String,Long]]()
    if(number<=0) return fileName2FileSizeArray.toArray
    var fileNames : Array[String] = null
    val config = new Configuration()
    val hdfs = FileSystem.get(URI.create(dir),config)
    try{
      val path = new Path(dir)
      hdfs.listStatus(path).filter(x =>{
        x.getPath.toString.endsWith(".bcp")==true
      }).sortBy(_.getAccessTime).map(x =>{
        fileName2FileSizeArray += Tuple2(x.getPath.toString,x.getLen)
      })
      if(fileName2FileSizeArray.size<=number){
        return fileName2FileSizeArray.toArray
      }else{
        return fileName2FileSizeArray.take(number).toArray
      }
    }catch {
      case e : Exception =>{
        e.printStackTrace()
      }
    }
    fileName2FileSizeArray.toArray
  }

  def deleteHDFSFile(dst: String):Boolean={
    var ret = false
    val config = new Configuration()
    val hdfs = FileSystem.get(URI.create(dst),config)
    try{
      val path = new Path(dst)
      ret = hdfs.delete(path)
    }catch {
      case e : Exception =>{
        e.printStackTrace()
      }
    }finally {

    }
    ret
  }
  def getDataNodeNum(): Int = {
    4
  }
  def getAllDirsAndSorted(path:Array[String]): ArrayBuffer[String] ={
    var allScanDirs = new ArrayBuffer[String]()
    path.foreach( x=>{
      if(checkHDFSDirExists(x)){
        listAllDirNames(x).foreach(y =>{
          allScanDirs.+=(y)
        })
      }else{
        println(s"scan dir: ${x} does not exit")
      }
    })
    allScanDirs.sorted
  }

  def listAllDirNames(dir:String):Array[String]={
    var dirNames: Array[String] = null
    val config = new Configuration()
    val hdfs = FileSystem.get(URI.create(dir),config)
    try{
      val path = new Path(dir)
      dirNames = hdfs.listStatus(path).filter(x =>{
        hdfs.isDirectory(x.getPath) == true
      }).map(_.getPath.toString)
    }catch {
      case e : Exception =>{
        e.printStackTrace()
      }
    }
    dirNames
  }
}
