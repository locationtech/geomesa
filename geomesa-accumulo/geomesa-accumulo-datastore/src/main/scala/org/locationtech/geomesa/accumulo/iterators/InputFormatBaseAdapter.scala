package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.hadoop.mapreduce.Job

import scala.util.{Failure, Success, Try}

object InputFormatBaseAdapter {

  object AccumuloVersion extends Enumeration {
    type AccumuloVersion = Value
    val V15, V16 = Value
  }

  import AccumuloVersion._
/*

  lazy val checkType: AccumuloVersion = {
    Try(classOf[InputFormatBase].getMethod("addIterator", classOf[Job], classOf[IteratorSetting])) match {
      case Failure(t: NoSuchMethodException) => V15
      case Success(m)                        => V16
    }
  }

  def addIterator(job: Job, cfg: IteratorSetting) = checkType match {
    case V15 => addIterator15(job, cfg)
    case V16 => addIterator16(job, cfg)
  }

  private def addIterator15(job: Job, cfg: IteratorSetting) = {
    //val method = classOf[InputFormatBase].getMethod("addIterator", classOf[])
  }

  private def addIterator16(job: Job, cfg: IteratorSetting) = {
    val method = classOf[InputFormatBase].getMethod("addIterator", classOf[Job], classOf[IteratorSetting])
    method.invoke(null, job, cfg)
  }
*/
}
