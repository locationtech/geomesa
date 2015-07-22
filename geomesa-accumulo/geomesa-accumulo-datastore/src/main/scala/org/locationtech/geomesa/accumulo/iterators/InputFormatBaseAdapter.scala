package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job

import scala.util.{Failure, Success, Try}

object InputFormatBaseAdapter {
  object AccumuloVersion extends Enumeration {
    type AccumuloVersion = Value
    val V15, V16, V17 = Value
  }

  import AccumuloVersion._


  lazy val checkType: AccumuloVersion = {
    Try(classOf[AccumuloInputFormat].getMethod("addIterator", classOf[Job], classOf[IteratorSetting])) match {
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
    val method = classOf[AccumuloInputFormat].getMethod("addIterator", classOf[Job], classOf[IteratorSetting])
    method.invoke(null, job, cfg)
  }

  def setConnectorInfo(job: Job, user: String, token: PasswordToken) = checkType match {
    case V15 => setConnectorInfo15(job, user, token)
    case V16 => setConnectorInfo16(job, user, token)
  }

  def setConnectorInfo15(job: Job, user: String, token: PasswordToken) = {
    val method = classOf[AccumuloInputFormat].getMethod("setConnectorInfo", classOf[Job], classOf[String], classOf[PasswordToken])
    method.invoke(null, job, user, token)
  }

  def setConnectorInfo16(job: Job, user: String, token: PasswordToken) = {
    val method = classOf[AccumuloInputFormat].getMethod("setConnectorInfo", classOf[Job], classOf[String], classOf[PasswordToken])
    method.invoke(null, job, user, token)
  }

  def setZooKeeperInstance(job: Job, instance: String, zookeepers: String) = checkType match {
    case V15 => setZooKeeperInstance15(job, instance, zookeepers)
    case V16 => setZooKeeperInstance16(job, instance, zookeepers)
  }

  def setZooKeeperInstance15(job: Job, instance: String, zookeepers: String) = {
    val method = classOf[AccumuloInputFormat].getMethod("setZooKeeperInstance", classOf[Job], classOf[String], classOf[String])
    method.invoke(null, job, instance, zookeepers)
  }


  def setZooKeeperInstance16(job: Job, instance: String, zookeepers: String) = {
    val method = classOf[AccumuloInputFormat].getMethod("setZooKeeperInstance", classOf[Job], classOf[String], classOf[String])
    method.invoke(null, job, instance, zookeepers)
  }

  def setScanAuthorizations(job: Job, authorizations: Authorizations): Unit = checkType match {
    case V15 => setScanAuthorizations15(job, authorizations)
    case V16 => setScanAuthorizations16(job, authorizations)
  }

  def setScanAuthorizations15(job: Job, authorizations: Authorizations): Unit = {
    val method = classOf[AccumuloInputFormat].getMethod("setScanAuthorizations", classOf[Job], classOf[Authorizations], classOf[String])
    method.invoke(null, job, authorizations)
  }

  def setScanAuthorizations16(job: Job, authorizations: Authorizations): Unit = {
    val method = classOf[AccumuloInputFormat].getMethod("setScanAuthorizations", classOf[Job], classOf[Authorizations], classOf[String])
    method.invoke(null, job, authorizations)
  }

}
