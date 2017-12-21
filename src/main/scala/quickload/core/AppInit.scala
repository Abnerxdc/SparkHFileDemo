package quickload.core

import org.apache.log4j.{Logger, PropertyConfigurator}

/**
  * Created by Administrator on 2017/7/14.
  */
object AppInit {

  val logger = Logger.getLogger(AppInit.getClass)

  def initApp(): Unit ={
    init("./conf")
  }

  private def init (iConfPath: String): Unit ={
    //设置log4j配置文件
    PropertyConfigurator.configure(iConfPath+"/log4j.properties")

    //设置配置文件路径
    logger.info("set application config path. path = "+iConfPath)

    ConfigManager.setConfPath(iConfPath)
  }

}
