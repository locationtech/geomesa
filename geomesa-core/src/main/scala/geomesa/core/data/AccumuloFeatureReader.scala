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

import geomesa.core.index._
import geomesa.core.iterators.DensityIterator
import org.apache.accumulo.core.data.Value
import org.geotools.data.{DataUtilities, Query, FeatureReader}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.factory.Hints.{IntegerKey, ClassKey}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class AccumuloFeatureReader(dataStore: AccumuloDataStore,
                            featureName: String,
                            query: Query,
                            indexSchemaFmt: String,
                            attributes: String,
                            sft: SimpleFeatureType)
  extends FeatureReader[SimpleFeatureType, SimpleFeature] {

  import AccumuloFeatureReader._

  val ff = CommonFactoryFinder.getFilterFactory2
  val indexSchema = SpatioTemporalIndexSchema(indexSchemaFmt, sft)
  val geometryPropertyName = sft.getGeometryDescriptor.getName.toString
  val encodedSFT           = DataUtilities.encodeType(sft)

  val projectedSFT =
    if(query.getHints.containsKey(DENSITY_KEY)) DataUtilities.createType(sft.getTypeName, "encodedraster:String,geom:Point:srid=4326")
    else sft

  val derivedQuery =
    if(query.getHints.containsKey(BBOX_KEY)) {
      val env = query.getHints.get(BBOX_KEY).asInstanceOf[ReferencedEnvelope]
      val q1 = new Query(sft.getTypeName, ff.bbox(ff.property(sft.getGeometryDescriptor.getLocalName), env))
      DataUtilities.mixQueries(q1, query, "geomesa.mixed.query")
    } else query

  val filterVisitor = new FilterToAccumulo(sft)
  val rewrittenCQL = filterVisitor.visit(derivedQuery)
  val cqlString = ECQL.toCQL(rewrittenCQL)

  val spatial = filterVisitor.spatialPredicate
  val temporal = filterVisitor.temporalPredicate

  lazy val bs = dataStore.createBatchScanner
  lazy val underlyingIter = if(query.getHints.containsKey(DENSITY_KEY)) {
    val width = query.getHints.containsKey(WIDTH_KEY).asInstanceOf[Integer]
    val height = query.getHints.containsKey(HEIGHT_KEY).asInstanceOf[Integer]
    indexSchema.query(bs, spatial, temporal, encodedSFT, Some(cqlString), true, width, height)
  } else {
    indexSchema.query(bs, spatial, temporal, encodedSFT, Some(cqlString), false)
  }

  lazy val iter =
    if(query.getHints.containsKey(DENSITY_KEY)) unpackDensityFeatures(underlyingIter)
    else underlyingIter.map { v => SimpleFeatureEncoder.decode(sft, v) }

  def unpackDensityFeatures(iter: Iterator[Value]) =
    iter.flatMap { i => DensityIterator.expandFeature(SimpleFeatureEncoder.decode(projectedSFT, i)) }

  override def getFeatureType = sft

  override def next() = iter.next()

  override def hasNext = iter.hasNext

  override def close() = bs.close()
}

object AccumuloFeatureReader {
  val DENSITY_KEY = new ClassKey(classOf[java.lang.Boolean])
  val WIDTH_KEY   = new IntegerKey(256)
  val HEIGHT_KEY  = new IntegerKey(256)
  val BBOX_KEY    = new ClassKey(classOf[ReferencedEnvelope])


}
