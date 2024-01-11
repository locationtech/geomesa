/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * Portions Copyright (c) 2021 The MITRE Corporation
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * This software was produced for the U. S. Government under Basic
 * Contract No. W56KGU-18-D-0004, and is subject to the Rights in
 * Noncommercial Computer Software and Noncommercial Computer Software
 * Documentation Clause 252.227-7014 (FEB 2012)
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.locationtech.geomesa.utils.io.WithClose

import java.util.Properties

object AccumuloClientConfig {

  val PasswordAuthType = "password"
  val KerberosAuthType = "kerberos"

  private val FileName: String = "accumulo-client.properties"

  /**
   * Search the classpath for Accumulo configuration files
   *
   * @return
   */
  def load(): Properties = {
    val props = new Properties()
    val loader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
    WithClose(loader.getResourceAsStream(FileName)) { is =>
      if (is != null) {
        props.load(is)
      }
    }
    props
  }
}
