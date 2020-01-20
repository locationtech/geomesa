/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable.spark

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

class BigtableInputFormat extends BigtableInputFormatBase with Configurable {

  /** The configuration. */
  private var conf: Configuration = _

  /**
    * Returns the current configuration.
    *
    * @return The current configuration.
    * @see org.apache.hadoop.conf.Configurable#getConf()
    */
  override def getConf: Configuration = conf

  /**
    * Sets the configuration. This is used to set the details for the tables to
    * be scanned.
    *
    * @param configuration The configuration to set.
    * @see   org.apache.hadoop.conf.Configurable#setConf(
    *        org.apache.hadoop.conf.Configuration)
    */
  override def setConf(configuration: Configuration): Unit = {
    this.conf = configuration
    val name = conf.get(TableInputFormat.INPUT_TABLE)
    if (name == null) {
      throw new IllegalArgumentException(s"Table name not configured under key '${TableInputFormat.INPUT_TABLE}'")
    }
    setName(TableName.valueOf(name))
    val rawScans = conf.getStrings(BigtableInputFormat.SCANS)
    if (rawScans == null) {
      throw new IllegalArgumentException(s"Scans not configured under key '${BigtableInputFormat.SCANS}'")
    } else if (rawScans.lengthCompare(1) < 0) {
      throw new IllegalArgumentException(
        s"Must be at least one scan configured under key '${BigtableInputFormat.SCANS}'")
    }
    val scans = new java.util.ArrayList[Scan](rawScans.length)
    rawScans.foreach(r => scans.add(BigtableInputFormatBase.stringToScan(r)))
    setScans(scans)
  }
}

object BigtableInputFormat {
  /** Job parameter that specifies the scan list. */
  val SCANS = "hbase.mapreduce.scans"
}
