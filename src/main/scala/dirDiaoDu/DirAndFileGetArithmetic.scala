package dirDiaoDu

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/12/20.
  */
object DirAndFileGetArithmetic {
  /**
    * 获取所有 将要 创建 temp 目录 的 文件名，和文件
    * @param curThreadRunningNum
    * @param dirArray
    * @return map（子目录，Array（文件））
    */
  def getDirAndFilesWillCreateTemp(curThreadRunningNum:Int,dirArray:ArrayBuffer[String]): scala.collection.mutable.HashMap[String,ArrayBuffer[String]] ={
    val dirAndFileNameMap = new mutable.HashMap[String,ArrayBuffer[String]]()
    var curMaxRetires = Model.minRetires
    var curMaxThreadNum = Model.lowerThreadLimit
    //最高峰配置
    if(HDFSUtils.getAllDirSize(Model.dir) >= Model.upperDirSizeInGB*1024){
      curMaxRetires = Model.maxRetires
      curMaxThreadNum = Model.upperThreadLimit
      //最低峰配置
    }else if(HDFSUtils.getAllDirSize(Model.dir) <= Model.lowerDirSizeInMB){
      curMaxRetires = Model.lowerRetires
    }
    //计算所需temp目录数量
    val tempDirNeedNum = curMaxThreadNum - curThreadRunningNum
    //获取要创建temp目录的子目录
    val dirWillCreateTemp = getPathByNum(tempDirNeedNum,dirArray,curMaxRetires)
    //形成 map（子目录 -> 将要移动的文件）
    dirWillCreateTemp.foreach(x =>{
      dirAndFileNameMap.+=(x -> countPreMoveTempDirFiles(x))
    })
    dirAndFileNameMap
  }

  /**
    * 获取 哪些 目录将要创建 temp 目录
    * @param num 需要的temp目录数量
    * @param dirArray 所有子目录的一个Array
    * @param curMaxRetires 本次 最大 重试 次数
    * @return
    */
  def getPathByNum(num:Int,dirArray:ArrayBuffer[String],curMaxRetires : Int): ArrayBuffer[String] ={
    val curDirNeedCreateTemp = new ArrayBuffer[String]()
    var curNeedNum = num
    //用于判断是否轮回一圈
    var exitFlag = dirArray.size
    if(Model.currentDataTypeIndex>=dirArray.size) Model.currentDataTypeIndex = 0
    while (curNeedNum > 0 && exitFlag > 0){
      //获取 本次处理类型
      val curDataType = dirArray(Model.currentDataTypeIndex)
      //放空处理
      if(!Model.dirsAndWaitTimeMap.keySet.contains(curDataType)) Model.dirsAndWaitTimeMap.+=(curDataType ->1)
      //扫描当前文件下所有bcp文件
      val fileNames = HDFSUtils.listAllFileNames(curDataType).filter(x => x.endsWith(".bcp"))
      //扫描当前目录下所有bcp文件大小
      val fileSize = HDFSUtils.getDirsFileSizeFilter(curDataType,".bcp").toInt
      if(fileNames.length >= Model.minFileListLength|| fileSize >= Model.minFileListSizeInMB*1024*1024 || Model.dirsAndWaitTimeMap.get(curDataType).get >= curMaxRetires){
        //重置等待次数
        Model.dirsAndWaitTimeMap.+=(curDataType -> 1)
        //temp目录需求量 -1
        curNeedNum -= 1
        //记录下当前目录
        curDirNeedCreateTemp.+=(curDataType)
      }else if(fileNames.length >0){
        //等待次数 +1
        Model.dirsAndWaitTimeMap.+=(curDataType->(Model.dirsAndWaitTimeMap.get(curDataType).get+1))
      }
      //当前类型指向下一个
      Model.currentDataTypeIndex +=1
      exitFlag -=1
    }
    curDirNeedCreateTemp
  }

  /**
    * 获取 当前目录下 可以组成一个 temp目录 的 文件名
    * @param curDirs 当前目录
    * @return
    */
  def countPreMoveTempDirFiles(curDirs : String):scala.collection.mutable.ArrayBuffer[String]={
    val fixedSizeFileNames = scala.collection.mutable.ArrayBuffer[String]()
    val fileNames : Array[Tuple2[String,Long]] = HDFSUtils.listNFileName2FileSize(curDirs,256)
    var sum = 0L
    fileNames.map(x=>{
      if(!x._1.endsWith("_COPYING") && x._1.endsWith(".bcp")){
        if(x._2 == 0){
          HDFSUtils.deleteHDFSFile(x._1)
        }else{
          sum += x._2
          if(fixedSizeFileNames.size == 0){
            fixedSizeFileNames += x._1
          }else if(sum /(1024*1024) < (128*HDFSUtils.getDataNodeNum)){
            fixedSizeFileNames += x._1
          }
        }
      }else{
        println("======do nothing==========")
      }
    })
    fixedSizeFileNames
  }
}
