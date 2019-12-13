/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2017-2019 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.mapreduce

import org.apache.accumulo.core.client.ClientConfiguration
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.mapreduce.Job
import org.locationtech.geomesa.accumulo.AccumuloVersion._

object InputFormatBaseAdapter {

  def setConnectorInfo(job: Job, user: String, token: AuthenticationToken) = accumuloVersion match {
    case V15 => setConnectorInfo15(job, user, token)
    case V16 => setConnectorInfo16(job, user, token)
    case _   => setConnectorInfo16(job, user, token)
  }

  def setConnectorInfo15(job: Job, user: String, token: AuthenticationToken) = {
    val method = Class.forName("org.apache.accumulo.core.client.mapreduce.InputFormatBase")
        .getMethod("setConnectorInfo", classOf[Job], classOf[String], classOf[AuthenticationToken])
    method.invoke(null, job, user, token)
  }

  def setConnectorInfo16(job: Job, user: String, token: AuthenticationToken) = {
    val method = classOf[AccumuloInputFormat]
        .getMethod("setConnectorInfo", classOf[Job], classOf[String], classOf[AuthenticationToken])
    method.invoke(null, job, user, token)
  }

  def setZooKeeperInstance(job: Job, instance: String, zookeepers: String, kerberos: Boolean = false) = accumuloVersion match {
    case V15 => setZooKeeperInstance15(job, instance, zookeepers)
    case V16 => setZooKeeperInstance16(job, instance, zookeepers, kerberos)
    case _   => setZooKeeperInstance16(job, instance, zookeepers, kerberos)
  }

  def setZooKeeperInstance15(job: Job, instance: String, zookeepers: String) = {
    val method = Class.forName("org.apache.accumulo.core.client.mapreduce.InputFormatBase")
        .getMethod("setZooKeeperInstance", classOf[Job], classOf[String], classOf[String])
    method.invoke(null, job, instance, zookeepers)
  }

  def setZooKeeperInstance16(job: Job, instance: String, zookeepers: String, kerberos: Boolean = false) = {
    val method = classOf[AccumuloInputFormat]
        .getMethod("setZooKeeperInstance", classOf[Job], classOf[ClientConfiguration])
    val cc = new ClientConfiguration().withInstance(instance).withZkHosts(zookeepers).withSasl(kerberos)
    method.invoke(null, job, cc)
  }

  def setScanAuthorizations(job: Job, authorizations: Authorizations): Unit = accumuloVersion match {
    case V15 => setScanAuthorizations15(job, authorizations)
    case V16 => setScanAuthorizations16(job, authorizations)
    case _   => setScanAuthorizations16(job, authorizations)
  }

  def setScanAuthorizations15(job: Job, authorizations: Authorizations): Unit = {
    val method = Class.forName("org.apache.accumulo.core.client.mapreduce.InputFormatBase")
        .getMethod("setScanAuthorizations", classOf[Job], classOf[Authorizations], classOf[String])
    method.invoke(null, job, authorizations)
  }

  def setScanAuthorizations16(job: Job, authorizations: Authorizations): Unit = {
    val method = classOf[AccumuloInputFormat].getMethod("setScanAuthorizations", classOf[Job], classOf[Authorizations])
    method.invoke(null, job, authorizations)
  }

}
