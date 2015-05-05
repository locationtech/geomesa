/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.kafka

import com.google.common.cache.Cache
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import com.vividsolutions.jts.index.quadtree.Quadtree
import org.geotools.data.collection.DelegateFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureSource}
import org.geotools.data.{FilteringFeatureReader, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.collection.DelegateFeatureIterator
import org.geotools.filter.FidFilterImpl
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.security.ContentFeatureSourceSecuritySupport
import org.locationtech.geomesa.utils.geotools.ContentFeatureSourceReTypingSupport
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.spatial.{BBOX, BinarySpatialOperator, Within}
import org.opengis.filter.{And, Filter, IncludeFilter, Or}

import scala.collection.JavaConversions._

abstract class KafkaConsumerFeatureSource(entry: ContentEntry,
                                 schema: SimpleFeatureType,
                                 query: Query,
                                 kf: KafkaConsumerFactory)
  extends ContentFeatureSource(entry, query)
  with ContentFeatureSourceSecuritySupport
  with ContentFeatureSourceReTypingSupport {

  case class FeatureHolder(sf: SimpleFeature, env: Envelope) {
    override def hashCode(): Int = sf.hashCode()

    override def equals(obj: scala.Any): Boolean = obj match {
      case other: FeatureHolder => sf.equals(other.sf)
      case _ => false
    }
  }

  def qt: Quadtree
  def features: Cache[String, FeatureHolder]

  override def getBoundsInternal(query: Query) =
    ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)

  override def buildFeatureType(): SimpleFeatureType = schema

  override def getCountInternal(query: Query): Int =
    getReaderInternal(query).getIterator.size

  override def getReaderInternal(query: Query): FR = addSupport(query, getReaderForFilter(query.getFilter))

  def getReaderForFilter(f: Filter): FR =
    f match {
      case o: Or             => or(o)
      case i: IncludeFilter  => include(i)
      case w: Within         => within(w)
      case b: BBOX           => bbox(b)
      case a: And            => and(a)
      case id: FidFilterImpl => fid(id)
      case _                 =>
        new FilteringFeatureReader[SimpleFeatureType, SimpleFeature](include(Filter.INCLUDE), f)
    }

  type DFR = DelegateFeatureReader[SimpleFeatureType, SimpleFeature]
  type DFI = DelegateFeatureIterator[SimpleFeature]

  def include(i: IncludeFilter) = new DFR(schema, new DFI(features.asMap().valuesIterator.map(_.sf)))

  def fid(ids: FidFilterImpl): FR = {
    val iter = ids.getIDs.flatMap(id => Option(features.getIfPresent(id.toString)).map(_.sf)).iterator
    new DFR(schema, new DFI(iter))
  }

  private val ff = CommonFactoryFinder.getFilterFactory2
  def and(a: And): FR = {
    // assume just one spatialFilter for now, i.e. 'bbox() && attribute equals ??'
    val (spatialFilter, others) = a.getChildren.partition(_.isInstanceOf[BinarySpatialOperator])
    val restFilter = ff.and(others)
    val filterIter = spatialFilter.headOption.map(getReaderForFilter).getOrElse(include(Filter.INCLUDE))
    new FilteringFeatureReader[SimpleFeatureType, SimpleFeature](filterIter, restFilter)
  }

  def or(o: Or): FR = {
    val readers = o.getChildren.map(getReaderForFilter).map(_.getIterator)
    val composed = readers.foldLeft(Iterator[SimpleFeature]())(_ ++ _)
    new DFR(schema, new DFI(composed))
  }

  def within(w: Within): FR = {
    val (_, geomLit) = splitBinOp(w)
    val geom = geomLit.evaluate(null).asInstanceOf[Geometry]
    val res = qt.query(geom.getEnvelopeInternal)
    val filtered = res.asInstanceOf[java.util.List[SimpleFeature]].filter(sf => geom.contains(sf.point))
    val fiter = new DFI(filtered.iterator)
    new DFR(schema, fiter)
  }

  def bbox(b: BBOX): FR = {
    val bounds = JTS.toGeometry(b.getBounds)
    val res = qt.query(bounds.getEnvelopeInternal)
    val fiter = new DFI(res.asInstanceOf[java.util.List[SimpleFeature]].iterator)
    new DFR(schema, fiter)
  }

  def splitBinOp(binop: BinarySpatialOperator): (PropertyName, Literal) =
    binop.getExpression1 match {
      case pn: PropertyName => (pn, binop.getExpression2.asInstanceOf[Literal])
      case l: Literal       => (binop.getExpression2.asInstanceOf[PropertyName], l)
    }
}
