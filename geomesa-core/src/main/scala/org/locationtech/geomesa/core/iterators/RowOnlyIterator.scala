/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.iterators

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
