package com.keks.sf

import com.keks.sf.SerializableConfiguration.tryOrIOException
import org.apache.hadoop.conf.Configuration

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import scala.util.control.NonFatal


// TODO use scala Try
/**
  * org.apache.hadoop.conf.Configuration is not Serializable.
  * Just a wrapper to achieve it.
  *
  * @param value org.apache.hadoop.conf.Configuration
  */
class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }
}

object SerializableConfiguration extends LogSupport {
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        error("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        error("Exception encountered", e)
        throw new IOException(e)
    }
  }
}