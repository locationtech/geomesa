/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.io

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor
import org.locationtech.geomesa.utils.io.fs.HadoopDelegate

import scala.util.control.NonFatal

/**
  * Hadoop utilities
  */
object HadoopUtils extends LazyLogging {

  private var krbRenewer: TicketLogin = _

  /**
    * Add a resource to the given conf
    *
    * @param conf conf
    * @param path resource path
    */
  def addResource(conf: Configuration, path: String): Unit = {
    // use our path handling logic, which is more robust than just passing paths to the config
    val delegate = if (PathUtils.isRemote(path)) { new HadoopDelegate(conf) } else { PathUtils }
    val handle = delegate.getHandle(path)
    if (!handle.exists) {
      logger.warn(s"Could not load configuration file at: $path")
    } else {
      WithClose(handle.open) { files =>
        files.foreach {
          case (None, is) => conf.addResource(is)
          case (Some(name), is) => conf.addResource(is, name)
        }
        conf.size() // this forces a loading of the resource files, before we close our file handle
      }
    }
  }

  /**
   * Checks for a secured cluster and creates a thread to periodically renew the kerberos ticket
   *
   * @return
   */
  def kerberosTicketRenewer(): Closeable = synchronized {
    if (krbRenewer == null) {
      krbRenewer = new TicketLogin()
    }
    krbRenewer.registrations += 1
    new KrbRegistration()
  }

  /**
   * Deregister a reference to the singleton kerberos ticket renewer
   */
  private def deregister(): Unit = synchronized {
    krbRenewer.registrations -= 1
    if (krbRenewer.registrations == 0) {
      CloseWithLogging(krbRenewer)
      krbRenewer = null
    }
  }

  /**
   * Runnable class to reload tickets
   */
  private class TicketLogin extends Runnable with Closeable with LazyLogging {

    private val executor = ExitingExecutor(new ScheduledThreadPoolExecutor(1))
    executor.scheduleAtFixedRate(this, 0, 10, TimeUnit.MINUTES)

    var registrations = 0

    override def run(): Unit = {
      try {
        logger.debug(s"Checking whether TGT needs renewing for ${UserGroupInformation.getCurrentUser}")
        logger.debug(s"Logged in from keytab? ${UserGroupInformation.getCurrentUser.isFromKeytab}")
        UserGroupInformation.getCurrentUser.checkTGTAndReloginFromKeytab()
      } catch {
        case NonFatal(e) => logger.warn("Error checking and renewing TGT", e)
      }
    }

    override def close(): Unit = executor.shutdown()
  }

  /**
   * Ensures that each registration is only closed at most once
   */
  private class KrbRegistration extends Closeable {
    private val closed = new AtomicBoolean(false)
    override def close(): Unit = if (closed.compareAndSet(false, true)) { deregister() }
  }
}
