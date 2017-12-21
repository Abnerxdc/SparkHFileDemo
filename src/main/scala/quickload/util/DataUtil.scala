package quickload.util

import java.util.Properties
import scala.collection.JavaConversions._

/**
  * Created by Administrator on 2017/7/14.
  */
object DataUtil {
  def getPropsByPrefix(mProp : Properties , iPrefix : String) : Properties ={
    val properties = new Properties()
    mProp.stringPropertyNames().foreach(propKey => {
      val iPrefixSize = iPrefix.length
      if(propKey.startsWith(iPrefix)){
        properties.setProperty(propKey.substring(iPrefixSize),mProp.getProperty(propKey))
      }
    })
    properties
  }



}
