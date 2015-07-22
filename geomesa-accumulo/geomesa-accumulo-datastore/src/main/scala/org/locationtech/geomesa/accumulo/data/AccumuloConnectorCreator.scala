/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.{BatchScanner, Scanner}
import org.locationtech.geomesa.accumulo.data.tables.GeoMesaTable

trait AccumuloConnectorCreator {

  /**
   * Gets the accumulo table name for the given table
   */
  def getTableName(featureName: String, table: GeoMesaTable): String

  /**
   * Gets the suggested number of threads for querying the given table
   */
  def getSuggestedThreads(featureName: String, table: GeoMesaTable): Int

  /**
   * Gets a single-range scanner for the given table
   */
  def getScanner(table: String): Scanner

  /**
   * Gets a batch scanner for the given table
   */
  def getBatchScanner(table: String, threads: Int): BatchScanner
}
