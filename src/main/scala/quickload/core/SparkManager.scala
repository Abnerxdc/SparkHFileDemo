package quickload.core

import java.io.File

import akka.event.slf4j.SLF4JLogging
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by Administrator on 2017/7/14.
  */
object SparkManager extends SLF4JLogging{
  private var sc : Option[SparkContext] = None


  def sparkInstance: Option[SparkContext] = sc


  def setSparkContext(createdContext: SparkContext): Unit = sc = Option(createdContext)

  private def getNewStreamingContext(batchDuration: Duration,checkpointDir: String): StreamingContext ={
    val ssc = new StreamingContext(sc.get, batchDuration)
    ssc
  }

  def sparkStandAloneContextInstance(generalConfig : Option[Map[String,String]],
                                     specificConfig:Map[String,String],
                                    jars: Seq[File]):SparkContext = {
    synchronized{
      sc.getOrElse(initStandAloneContext(generalConfig,specificConfig,jars))
    }
  }

  def sparkClusterContextInstance(specificConfig:Map[String,String],jars: Seq[String] = Seq[String]()):SparkContext ={
    synchronized{
      sc.getOrElse(initClusterContext(specificConfig,jars))
    }
  }

  private def initStandAloneContext(generalConfig : Option[Map[String,String]],
                                    specificConfig:Map[String,String],
                                    jars: Seq[File]):SparkContext = {
    sc = Some(SparkContext.getOrCreate(mapToSparkConf(generalConfig,specificConfig)))
    jars.foreach(f => sc.get.addJar(f.getAbsolutePath))
    sc.get
  }

  private def initClusterContext(specificConfig:Map[String,String],
                                jars: Seq[String] = Seq[String]()):SparkContext ={
    sc = Some(SparkContext.getOrCreate(mapToSparkConf(None,specificConfig)))
    jars.foreach(f => sc.get.addJar(f))
    sc.get
  }

  private def mapToSparkConf(generalConfig: Option[Map[String,String]],specificConfig:Map[String,String]):SparkConf ={
    val conf = new SparkConf()
    if(generalConfig.isDefined){
      generalConfig.get.foreach{
        case (key,value) => {conf.set(key, value)}
      }
    }
    specificConfig.foreach{case (key, value) =>conf.set(key,value) }
    conf
  }

  def destroySparkContext(): Unit ={
    synchronized{
      sc.fold(log.warn("Spark Context is Empty")){sparkContext =>
        try{
          log.info("Stopping SparkContext with name: "+sparkContext.appName)
          sparkContext.stop()
          log.info("Stopping SparkContext with name: "+sparkContext.appName)
        }finally{
          sc = None
        }
      }
    }
  }

}
