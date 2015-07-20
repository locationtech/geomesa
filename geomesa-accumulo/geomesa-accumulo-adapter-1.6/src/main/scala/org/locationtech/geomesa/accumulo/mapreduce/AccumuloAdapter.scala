package org.locationtech.geomesa.accumulo.mapreduce

import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.lib.impl.{InputConfigurator, ConfiguratorBase => ImplConfiguratorBase}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.conf.Configuration


object AccumuloAdapter {
  def setZooKeeperInstance(conf: Configuration, instance: String, zooKeepers: String) = {
    ImplConfiguratorBase.setZooKeeperInstance(classOf[AccumuloInputFormat], conf,
      new ClientConfiguration().withInstance(instance).withZkHosts(zooKeepers))
  }

  def setConnectorInfo(conf: Configuration, user: String, password: String) = {
    ImplConfiguratorBase.setConnectorInfo(classOf[AccumuloInputFormat], conf, user,
      new PasswordToken(password.getBytes))
  }

  def setScanAuthorizations(conf: Configuration, auths: Authorizations) = {
    InputConfigurator.setScanAuthorizations(classOf[AccumuloInputFormat], conf, auths)
  }
}
