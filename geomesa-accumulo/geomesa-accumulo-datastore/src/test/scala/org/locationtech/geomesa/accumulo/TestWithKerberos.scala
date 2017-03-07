/** *********************************************************************
  * Crown Copyright (c) 2016 Dstl
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  * ************************************************************************/

package org.locationtech.geomesa.accumulo

import org.specs2.mutable.Specification

import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer
import org.apache.kerby.util.NetworkUtil

import java.io.File


/**
  * Trait to simplify tests that require a Key Distribution Center (KDC)
  */
trait TestWithKerberos extends Specification {

  // File paths, create workDir if doesn't already exist
  // TODO: verify this path is ok
  // TODO: should delete workDir afterwards?
  // TODO: should stop kdc after testing too?
  val workDir = new File("." + File.separator + "kerberos")
  if(!workDir.exists) {
    workDir.mkdir
  }
  val keytabFilename = new File(workDir, "keytab").getPath

  // Configure KDC
  val kdc = new SimpleKdcServer
  kdc.setKdcHost("localhost")
  kdc.setKdcRealm("EXAMPLE.COM")
  kdc.setWorkDir(workDir)
  kdc.setKdcTcpPort(NetworkUtil.getServerPort)
  kdc.setAllowUdp(false)
  kdc.setAllowTcp(true)

  // Initialise and start KDC
  kdc.init()
  kdc.start()

  // Add our user
  kdc.createPrincipal("user@EXAMPLE.COM", "password")

  // Write user keytab to file
  {
    val keytabFile = new File(keytabFilename)
    kdc.exportPrincipals(keytabFile)
    keytabFile.flush()
  }

  // Hadoop code seems to do this with the comment
  // Will be fixed in next Kerby version.
  Thread.sleep(1000)

}