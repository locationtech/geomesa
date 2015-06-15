/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.UUID

import org.apache.accumulo.core.client.{IteratorSetting, ScannerBase}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator, WrappingIterator}

object RowOnlyIterator {
  def setupRowOnlyIterator(scanner: ScannerBase, priority: Int) {
    val iteratorName = "RowOnlyIterator-" + UUID.randomUUID.toString.subSequence(0, 5)
    scanner.addScanIterator(new IteratorSetting(priority, iteratorName, classOf[RowOnlyIterator]))
  }
}

class RowOnlyIterator
    extends WrappingIterator {
  @Override
  override def getTopKey: Key = new Key(super.getTopKey.getRow)

  @Override
  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = null
}
