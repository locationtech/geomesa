/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.core.index

import java.util.Map.Entry

import org.apache.accumulo.core.data.{Value, Key}
import org.geotools.data.Query
import org.locationtech.geomesa.core.data.{FilterToAccumulo, AccumuloConnectorCreator}
import org.locationtech.geomesa.core.util.{SelfClosingBatchScanner, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Id

import scala.collection.JavaConversions._

class RecordIdxStrategy extends Strategy {

  override def execute(acc: AccumuloConnectorCreator, iqp: IndexQueryPlanner, featureType: SimpleFeatureType, query: Query, filterVisitor: FilterToAccumulo, output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val idFilter = query.getFilter.asInstanceOf[Id]
    val recordScanner = acc.createRecordScanner(featureType)
    val ranges = idFilter.getIdentifiers.map { id =>
      org.apache.accumulo.core.data.Range.exact(id.toString)
    }
    recordScanner.setRanges(ranges)

    SelfClosingBatchScanner(recordScanner)

  }
}