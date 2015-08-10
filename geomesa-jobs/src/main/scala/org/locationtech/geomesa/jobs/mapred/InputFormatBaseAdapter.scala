package org.locationtech.geomesa.jobs.mapred

import org.apache.accumulo.core.client.mapred.AccumuloInputFormat
import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, PasswordToken}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Level
import org.locationtech.geomesa.accumulo.AccumuloVersion._

object InputFormatBaseAdapter {

  def setConnectorInfo(job: JobConf, user: String, token: PasswordToken) = accumuloVersion match {
    case V15 => setConnectorInfo15(job, user, token)
    case V16 => setConnectorInfo16(job, user, token)
    case _   => setConnectorInfo16(job, user, token)
  }

  def setConnectorInfo15(job: JobConf, user: String, token: PasswordToken) = {
    val method = Class.forName("org.apache.accumulo.core.client.mapred.InputFormatBase")
        .getMethod("setConnectorInfo", classOf[JobConf], classOf[String], classOf[AuthenticationToken])
    method.invoke(null, job, user, token)
  }

  def setConnectorInfo16(job: JobConf, user: String, token: PasswordToken) = {
    val method = classOf[AccumuloInputFormat]
        .getMethod("setConnectorInfo", classOf[JobConf], classOf[String], classOf[AuthenticationToken])
    method.invoke(null, job, user, token)
  }

  def setZooKeeperInstance(job: JobConf, instance: String, zookeepers: String) = accumuloVersion match {
    case V15 => setZooKeeperInstance15(job, instance, zookeepers)
    case V16 => setZooKeeperInstance16(job, instance, zookeepers)
    case _   => setZooKeeperInstance16(job, instance, zookeepers)
  }

  def setZooKeeperInstance15(job: JobConf, instance: String, zookeepers: String) = {
    val method = Class.forName("org.apache.accumulo.core.client.mapred.InputFormatBase")
        .getMethod("setZooKeeperInstance", classOf[JobConf], classOf[String], classOf[String])
    method.invoke(null, job, instance, zookeepers)
  }

  def setZooKeeperInstance16(job: JobConf, instance: String, zookeepers: String) = {
    val method = classOf[AccumuloInputFormat]
        .getMethod("setZooKeeperInstance", classOf[JobConf], classOf[String], classOf[String])
    method.invoke(null, job, instance, zookeepers)
  }

  def setScanAuthorizations(job: JobConf, authorizations: Authorizations): Unit = accumuloVersion match {
    case V15 => setScanAuthorizations15(job, authorizations)
    case V16 => setScanAuthorizations16(job, authorizations)
    case _   => setScanAuthorizations16(job, authorizations)
  }

  def setScanAuthorizations15(job: JobConf, authorizations: Authorizations): Unit = {
    val method = Class.forName("org.apache.accumulo.core.client.mapred.InputFormatBase")
        .getMethod("setScanAuthorizations", classOf[JobConf], classOf[Authorizations], classOf[String])
    method.invoke(null, job, authorizations)
  }

  def setScanAuthorizations16(job: JobConf, authorizations: Authorizations): Unit = {
    val method = classOf[AccumuloInputFormat]
        .getMethod("setScanAuthorizations", classOf[JobConf], classOf[Authorizations], classOf[String])
    method.invoke(null, job, authorizations)
  }

  def setLogLevel(job: JobConf, level: Level) = accumuloVersion match {
    case V15 => setLogLevel15(job, level)
    case V16 => setLogLevel16(job, level)
    case _   => setLogLevel16(job, level)
  }

  def setLogLevel15(job: JobConf, level: Level) = {
    val method = Class.forName("org.apache.accumulo.core.client.mapred.InputFormatBase")
        .getMethod("setLogLevel", classOf[JobConf], classOf[Level])
    method.invoke(null, job, level)
  }

  def setLogLevel16(job: JobConf, level: Level) = {
    val method = classOf[AccumuloInputFormat].getMethod("setLogLevel", classOf[JobConf], classOf[Level])
    method.invoke(null, job, level)
  }

}
