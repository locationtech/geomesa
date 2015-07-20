package org.locationtech.geomesa.accumulo.mapreduce

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.lib.util.{InputConfigurator, ConfiguratorBase}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.conf.Configuration


object AccumuloAdapter {
  def setZooKeeperInstance(conf: Configuration, instance: String, zooKeepers: String) = {
    ConfiguratorBase.setZooKeeperInstance(classOf[AccumuloInputFormat], conf,
      instance, zooKeepers)
  }

  def setConnectorInfo(conf: Configuration, user: String, password: String) = {
    ConfiguratorBase.setConnectorInfo(classOf[AccumuloInputFormat], conf, user,
      new PasswordToken(password.getBytes))
  }

  def setScanAuthorizations(conf: Configuration, auths: Authorizations) = {
    InputConfigurator.setScanAuthorizations(classOf[AccumuloInputFormat], conf, auths)
  }
}
