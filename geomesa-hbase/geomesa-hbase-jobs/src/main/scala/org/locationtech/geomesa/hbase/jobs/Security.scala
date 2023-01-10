/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fb054a34dc (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> fb054a34dc (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> b39bd292d4 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fb054a34dc (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.jobs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.{HBaseGeoMesaKeyTab, HBaseGeoMesaPrincipal}

import java.io.StringWriter
import java.security.PrivilegedAction

object Security extends LazyLogging {

  private def asString(conf: Configuration): String = {
    val writer = new StringWriter()
    Configuration.dumpConfiguration(conf, writer)
    writer.toString
  }

  // We cannot rely on a current user to be available when we run any HBase tasks
  // on the worker nodes there will be no security configured, or if that is
  // for some reason HBase would peek the current user which is not always a login user (root on Databriks)
  def doAuthorized[A](conf: Configuration)(action: => A): A = {
    logger.debug(s"Running doAuthorized on ${Thread.currentThread()} with config: ${asString(conf)}")
    // the keytab should be available as local file on master an executors
    // we might also convert keytab as base64 string, replicate via conf property and write into a local file
    val principal = conf.get(HBaseGeoMesaPrincipal)
    val keytab = conf.get(HBaseGeoMesaKeyTab)
    if (principal != null && keytab != null) {
      logger.debug(s"Logging in as $principal using keytab $keytab")
      // setting config is required so Hadoop lib would know that security is enabled
      UserGroupInformation.setConfiguration(conf)
      val user = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
      user.doAs {
        new PrivilegedAction[A] {
          override def run() = {
            logger.debug(s"Execution action under ${UserGroupInformation.getLoginUser} user...")
            val result = action
            logger.debug(s"The action is complete, finishing secured session for ${UserGroupInformation.getLoginUser} user...")
            result
          }
        }
      }
    } else {
      action
    }
  }
}
