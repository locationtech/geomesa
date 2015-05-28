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

import java.io.Serializable
import java.{lang => jl, util => ju}

import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.store.{ContentEntry, ContentFeatureSource}
import org.geotools.data.{FilteringFeatureReader, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.FidFilterImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.kafka.KafkaDataStore.FeatureSourceFactory
import org.locationtech.geomesa.kafka.consumer.KafkaConsumerFactory
import org.locationtech.geomesa.security.ContentFeatureSourceSecuritySupport
import org.locationtech.geomesa.utils.geotools.ContentFeatureSourceReTypingSupport
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.{DFI, DFR, FR}
import org.locationtech.geomesa.utils.index.QuadTreeFeatureStore
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.spatial.{BBOX, BinarySpatialOperator, Within}
import org.opengis.filter.{And, Filter, IncludeFilter, Or}

import scala.collection.JavaConversions._
import scala.collection.mutable

abstract class KafkaConsumerFeatureSource(entry: ContentEntry,
                                          schema: SimpleFeatureType,
                                          query: Query)
  extends ContentFeatureSource(entry, query)
  with ContentFeatureSourceSecuritySupport
  with ContentFeatureSourceReTypingSupport {

  override def getBoundsInternal(query: Query) =
    ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)

  override def buildFeatureType(): SimpleFeatureType = schema

  override def getCountInternal(query: Query): Int =
    getReaderInternal(query).getIterator.size

  override val canFilter: Boolean = true

  override def getReaderInternal(query: Query): FR = addSupport(query, getReaderForFilter(query.getFilter))

  def getReaderForFilter(f: Filter): FR
}

case class FeatureHolder(sf: SimpleFeature, env: Envelope) {
  override def hashCode(): Int = sf.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: FeatureHolder => sf.equals(other.sf)
    case _ => false
  }
}

trait KafkaConsumerFeatureCache extends QuadTreeFeatureStore {

  def features: mutable.Map[String, FeatureHolder]

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

  def include(i: IncludeFilter) = new DFR(sft, new DFI(features.valuesIterator.map(_.sf)))

  def fid(ids: FidFilterImpl): FR = {
    val iter = ids.getIDs.flatMap(id => features.get(id.toString).map(_.sf)).iterator
    new DFR(sft, new DFI(iter))
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
    new DFR(sft, new DFI(composed))
  }
}

object KafkaConsumerFeatureSourceFactory {

  val EXPIRY            = new Param("expiry", classOf[jl.Boolean], "Expiry", false, false)
  val EXPIRATION_PERIOD = new Param("expirationPeriod", classOf[jl.Long], "Expiration Period in milliseconds", false)

  def apply(brokers: String, zk: String, params: ju.Map[String, Serializable]): FeatureSourceFactory = {

    lazy val expirationPeriod: Option[Long] = {
      val expiry = Option(EXPIRY.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)
      if (expiry) {
        Option(EXPIRATION_PERIOD.lookUp(params)).map(_.toString.toLong).orElse(Some(0L))
      } else {
        None
      }
    }

    (entry: ContentEntry, schemaManager: KafkaDataStoreSchemaManager) => {
      val kf = new KafkaConsumerFactory(brokers, zk)
      val fc = schemaManager.getFeatureConfig(entry.getTypeName)

      fc.replayConfig match {
        case None =>
          new LiveKafkaConsumerFeatureSource(entry, fc.sft, fc.topic, kf, expirationPeriod)

        case Some(rc) =>
          val replaySFT = fc.sft
          val liveSFT = schemaManager.getLiveFeatureType(replaySFT)
            .getOrElse(throw new IllegalArgumentException(
              "Cannot create Replay FeatureSource because SFT has not been properly prepared."))
          new ReplayKafkaConsumerFeatureSource(entry, replaySFT, liveSFT, fc.topic, kf, rc)
      }
    }
  }
}
