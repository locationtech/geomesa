package org.locationtech.geomesa.accumulo.mapred

import org.apache.accumulo.core.client.mapred.{AccumuloInputFormat, InputFormatBase}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.Level


object AccumuloAdapter {
  def isConnectorInfoSet(conf: JobConf) = {
    // JNH: This is the one exception to the 'mapred' only.
    org.apache.accumulo.core.client.mapreduce.lib.util.ConfiguratorBase.isConnectorInfoSet(
      classOf[AccumuloInputFormat],
      conf
    )
  }

  def setZooKeeperInstance(conf: JobConf, instance: String, zooKeepers: String) = {
    InputFormatBase.setZooKeeperInstance(conf, instance, zooKeepers)
  }

  def setConnectorInfo(conf: JobConf, user: String, password: String) = {
    InputFormatBase.setConnectorInfo(conf, user, new PasswordToken(password.getBytes))
  }

  def setInputTableName(conf: JobConf, table: String) = {
    InputFormatBase.setInputTableName(conf, table)
  }

  def setScanAuthorizations(conf: JobConf, auths: Authorizations) = {
    InputFormatBase.setScanAuthorizations(conf, auths)
  }

  def setLogLevel(conf: JobConf, level: Level) = {
    InputFormatBase.setLogLevel(conf, level)
  }
}

