/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import java.io.Serializable
import java.{util => ju}

import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.store.{ContentEntry, ContentFeatureSource}
import org.geotools.data.{FilteringFeatureReader, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.NameImpl
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.FidFilterImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.kafka.KafkaDataStore.FeatureSourceFactory
import org.locationtech.geomesa.kafka.consumer.KafkaConsumerFactory
import org.locationtech.geomesa.security.ContentFeatureSourceSecuritySupport
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.index.QuadTreeFeatureStore
import org.opengis.feature.`type`.Name
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

  override def getBoundsInternal(query: Query) = KafkaConsumerFeatureSource.wholeWorldBounds

  override def buildFeatureType(): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.init(schema)
    builder.setNamespaceURI(getDataStore.getNamespaceURI)
    schema.getUserData.foreach { case (k, v) => builder.userData(k, v)}
    val sft = builder.buildFeatureType()
    sft
  }

  override def getCountInternal(query: Query): Int = getReaderInternal(query).getIterator.length

  override val canFilter: Boolean = true

  override def getReaderInternal(query: Query): FR = addSupport(query, getReaderForFilter(query.getFilter))

  def getReaderForFilter(f: Filter): FR

  lazy val fqName = new NameImpl(getDataStore.getNamespaceURI, getSchema.getName.getLocalPart)

  override def getName: Name = fqName
}

object KafkaConsumerFeatureSource {
  lazy val wholeWorldBounds = ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), CRS_EPSG_4326)
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
  private val ff = CommonFactoryFinder.getFilterFactory2

  // optimized for filter.include
  def size(f: Filter): Int = {
    if (f == Filter.INCLUDE) {
      features.size
    } else {
      getReaderForFilter(f).getIterator.length
    }
  }

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

  def apply(brokers: String, zk: String, params: ju.Map[String, Serializable]): FeatureSourceFactory = {

    lazy val expirationPeriod: Option[Long] = {
      Option(KafkaDataStoreFactoryParams.EXPIRATION_PERIOD.lookUp(params)).map(_.toString.toLong).filter(_ > 0)
    }

    val cleanUpCache: Boolean = {
      Option(KafkaDataStoreFactoryParams.CLEANUP_LIVE_CACHE.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)
    }

    (entry: ContentEntry, schemaManager: KafkaDataStoreSchemaManager) => {
      val kf = new KafkaConsumerFactory(brokers, zk)
      val fc = schemaManager.getFeatureConfig(entry.getTypeName)

      fc.replayConfig match {
        case None =>
          new LiveKafkaConsumerFeatureSource(entry, fc.sft, fc.topic, kf, expirationPeriod, cleanUpCache)

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
