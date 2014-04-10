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


package geomesa.core.data

import com.vividsolutions.jts.geom._
import geomesa.core.index._
import org.geotools.data.{DataUtilities, Query, FeatureReader}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class AccumuloFeatureReader(dataStore: AccumuloDataStore,
                            featureName: String,
                            query: Query,
                            indexSchemaFmt: String,
                            attributes: String,
                            sft: SimpleFeatureType)
  extends FeatureReader[SimpleFeatureType, SimpleFeature] {

  import collection.JavaConversions._

  val ff = CommonFactoryFinder.getFilterFactory2
  val indexSchema = SpatioTemporalIndexSchema(indexSchemaFmt, sft)
  val geometryPropertyName = sft.getGeometryDescriptor.getName.toString
  val dtgStartField        = sft.getUserData.getOrElse(SF_PROPERTY_START_TIME, SF_PROPERTY_START_TIME).asInstanceOf[String]
  val dtgEndField          = sft.getUserData.getOrElse(SF_PROPERTY_END_TIME, SF_PROPERTY_END_TIME).asInstanceOf[String]
  val encodedSFT           = DataUtilities.encodeType(sft)

  val filterVisitor = new FilterToAccumulo(sft)
  val rewrittenCQL = filterVisitor.visit(query)
  val cqlString = ECQL.toCQL(rewrittenCQL)

  // run the query
  val bs = dataStore.createBatchScanner

  val spatial = filterVisitor.spatialPredicate
  val temporal = filterVisitor.temporalPredicate
  lazy val iterValues = indexSchema.query(bs, spatial, temporal, encodedSFT, Some(cqlString))

  override def getFeatureType = sft

  override def next() = SimpleFeatureEncoder.decode(getFeatureType, iterValues.next())

  override def hasNext = iterValues.hasNext

  override def close() = bs.close()
}

object AccumuloFeatureReader {
  val latLonGeoFactory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 4326)
}
