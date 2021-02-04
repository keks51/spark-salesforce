package com.keks.sf.soap

import com.keks.sf.implicits.RichTry
import com.keks.sf.{LogSupport, SfOptions}
import com.sforce.soap.partner.PartnerConnection

import scala.util.Try


object SoapQueryExecutorClassLoader extends LogSupport {

  def loadClass(className: String,
                sfOptions: SfOptions,
                soapConnection: PartnerConnection,
                executorName: String): SoapQueryExecutor = {


    val classLoader: ClassLoader = Thread.currentThread().getContextClassLoader
    Try {
      val loadedClass = classLoader.loadClass(className)
      val constructor = loadedClass
        .getConstructor(classOf[SfOptions], classOf[PartnerConnection], classOf[String])
      constructor.newInstance(sfOptions,
                              soapConnection,
                              executorName).asInstanceOf[SoapQueryExecutor] match {
        case inst: SoapQueryExecutor => inst
        case _ => throw new UnsupportedOperationException(
          s"Query Executor $className is not supported")
      }
    }.onFailure { case ex: ClassNotFoundException =>
      throw new ClassNotFoundException(s"Classloader cannot find class: '$className'", ex)
    }

  }

}
