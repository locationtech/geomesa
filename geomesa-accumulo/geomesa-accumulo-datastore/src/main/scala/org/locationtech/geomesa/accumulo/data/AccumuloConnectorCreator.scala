/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.{BatchScanner, Connector, Scanner}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index.AccumuloWritableIndex

trait AccumuloConnectorCreator {

  /**
   * Handle to connector
   *
   * @return connector
   */
  def connector: Connector

  /**
   * Gets the accumulo table name for the given table
   */
  def getTableName(featureName: String, index: AccumuloWritableIndex): String

  /**
   * Gets the suggested number of threads for querying the given table
   */
  def getSuggestedThreads(featureName: String, index: AccumuloFeatureIndex): Int

  /**
   * Gets a single-range scanner for the given table
   */
  def getScanner(table: String): Scanner

  /**
   * Gets a batch scanner for the given table
   */
  def getBatchScanner(table: String, threads: Int): BatchScanner
}
