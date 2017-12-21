package dirDiaoDu

import java.util.concurrent.{ExecutorService, Executors, ThreadPoolExecutor}

/**
  * Created by Administrator on 2017/12/20.
  */
object MainControl {
val realtimePrePool :ExecutorService = Executors.newFixedThreadPool(20)
  def scanWatchDirAndSubmitRealtimeExactor(): Unit ={
    val dirArray = HDFSUtils.getAllDirsAndSorted(Model.dir)
    //第一次运行
    if(Model.isFirstTimeRunning == 1 ){
      Model.isFirstTimeRunning = 0
      //获取所有temp目录（原逻辑）
      //调用处理逻辑（原逻辑）
    }
    //获取正在运行线程数
    val curThreadRunNum = realtimePrePool.asInstanceOf[ThreadPoolExecutor].getQueue.size()+realtimePrePool.asInstanceOf[ThreadPoolExecutor].getActiveCount
    //获取算法类的map（目录，Array（文件））
    val dirAndFilesNameMap = DirAndFileGetArithmetic.getDirAndFilesWillCreateTemp(curThreadRunNum,dirArray)
    //生成 temp 目录 并 移动文件
    dirAndFilesNameMap.foreach(x =>{
      println("mulu : "+x._1)
      println("wenjian : "+x._2)
    })
    //获取所有temp目录（原逻辑）
    //调用处理逻辑（原逻辑）
  }
}
