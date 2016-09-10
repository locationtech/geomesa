/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.util

import java.util.Map.Entry
import java.util.concurrent.TimeUnit
import java.util.{Iterator => jIterator}

import org.apache.accumulo.core.client.{IteratorSetting, Scanner}
import org.apache.accumulo.core.data.{Key, Range, Value}
import org.apache.hadoop.io.Text

import scala.collection.JavaConversions._

object EmptyScanner extends Scanner {

  override def getRange: Range = ???
  override def setRange(range: Range): Unit = {}

  override def setTimeOut(timeOut: Int): Unit = {}
  override def getTimeOut: Int = ???

  override def getTimeout(timeUnit: TimeUnit): Long = ???
  override def setTimeout(timeOut: Long, timeUnit: TimeUnit): Unit = {}

  override def setBatchSize(size: Int): Unit = {}
  override def getBatchSize: Int = ???

  override val iterator: jIterator[Entry[Key, Value]] = Iterator.empty

  override def enableIsolation(): Unit = {}
  override def disableIsolation(): Unit = {}

  override def addScanIterator(cfg: IteratorSetting): Unit = {}
  override def updateScanIteratorOption(iteratorName: String, key: String, value: String): Unit = {}
  override def removeScanIterator(iteratorName: String): Unit = {}
  override def clearScanIterators(): Unit = {}

  override def fetchColumn(colFam: Text, colQual: Text): Unit = {}
  override def fetchColumnFamily(col: Text): Unit = {}
  override def clearColumns(): Unit = {}

  override def close(): Unit = {}

  // added in accumulo 1.6 - don't user override so 1.5 compiles
  def getReadaheadThreshold: Long = ???
  def setReadaheadThreshold(batches: Long): Unit = {}

  // added in Accumulo 1.7 
  def fetchColumn(x$1: org.apache.accumulo.core.client.IteratorSetting.Column): Unit = ???
  def getAuthorizations(): org.apache.accumulo.core.security.Authorizations = ???
}
