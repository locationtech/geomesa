/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.index.conf.QueryProperties.QueryExactCountMaxFeatures
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureSource.{DelegatingResourceInfo, GeoMesaQueryCapabilities}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.sort.SortBy
import org.opengis.referencing.crs.CoordinateReferenceSystem

import java.awt.RenderingHints.Key
import java.net.URI
import java.util
import java.util.Collections
import scala.util.Try

class GeoMesaFeatureSource(val ds: GeoMeasBaseStore, val sft: SimpleFeatureType)
    extends SimpleFeatureSource with LazyLogging {

  lazy private val hints = Collections.unmodifiableSet(Collections.emptySet[Key])

  override def getSchema: SimpleFeatureType = sft

  /**
    * The default behavior for getCount is to use estimated statistics if available, or -1 to indicate
    * that the operation would be expensive (@see org.geotools.data.FeatureSource#getCount(org.geotools.data.Query)).
    *
    * Since users may want <b>exact</b> counts, there are two ways to force exact counts:
    *   1. use the system property "geomesa.force.count"
    *   2. use the EXACT_COUNT query hint
    *
    * @param query query
    * @return
    */
  override def getCount(query: Query): Int = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.index.conf.QueryProperties.QueryExactCount

    // configure the query hints
    val hints = ds.getFeatureReader(sft, Transaction.AUTO_COMMIT, query).hints
    val useExactCount = hints.isExactCount.getOrElse(QueryExactCount.get.toBoolean)

    val count = if (useExactCount &&
      !query.isMaxFeaturesUnlimited &&
      query.getMaxFeatures < QueryExactCountMaxFeatures.get.toInt) {
      SelfClosingIterator(getFeatures(query)).size
    } else {
      val statsCount = ds.stats.getCount(getSchema, query.getFilter, useExactCount, hints).getOrElse(-1L)
      if (query.isMaxFeaturesUnlimited) {
        statsCount
      } else {
        math.min(statsCount, query.getMaxFeatures)
      }
    }

    if (count > Int.MaxValue) {
      logger.warn(s"Truncating count $count to Int.MaxValue (${Int.MaxValue})")
      Int.MaxValue
    } else {
      count.toInt
    }
  }

  override def getBounds: ReferencedEnvelope = getBounds(new Query(sft.getTypeName, Filter.INCLUDE))

  override def getBounds(query: Query): ReferencedEnvelope = ds.stats.getBounds(getSchema, query.getFilter)

  override def getQueryCapabilities: QueryCapabilities = GeoMesaQueryCapabilities

  override def getFeatures: SimpleFeatureCollection = getFeatures(Filter.INCLUDE)

  override def getFeatures(filter: Filter): SimpleFeatureCollection =
    getFeatures(new Query(sft.getTypeName, filter))

  override def getFeatures(original: Query): SimpleFeatureCollection = {
    val query = if (original.getTypeName != null) { original } else {
      logger.debug(s"Received Query with null typeName, setting to: ${sft.getTypeName}")
      val nq = new Query(original)
      nq.setTypeName(sft.getTypeName)
      nq
    }
    new GeoMesaFeatureCollection(this, query)
  }

  override def getName: Name = getSchema.getName

  override def getDataStore: DataStore = ds

  override def getSupportedHints: java.util.Set[Key] = hints

  override def getInfo: ResourceInfo = new DelegatingResourceInfo(this)

  override def addFeatureListener(listener: FeatureListener): Unit = throw new NotImplementedError()

  override def removeFeatureListener(listener: FeatureListener): Unit = throw new NotImplementedError()
}

object GeoMesaFeatureSource {

  object GeoMesaQueryCapabilities extends QueryCapabilities {
    override def isOffsetSupported = false
    override def isReliableFIDSupported = true
    override def isUseProvidedFIDSupported = true
    override def supportsSorting(sortAttributes: SortBy*) = true
  }

  class DelegatingResourceInfo(source: SimpleFeatureSource) extends ResourceInfo {

    import scala.collection.JavaConverters._

    private val keywords = Collections.unmodifiableSet((Set("features", getName) ++ source.getSchema.getKeywords).asJava)

    override def getName: String = source.getSchema.getName.getURI

    override def getTitle: String = source.getSchema.getName.getLocalPart

    override def getDescription: String = null

    override def getKeywords: util.Set[String] = keywords

    override def getSchema: URI = Try(new URI(source.getSchema.getName.getNamespaceURI)).getOrElse(null)

    override def getCRS: CoordinateReferenceSystem = source.getSchema.getCoordinateReferenceSystem

    override def getBounds: ReferencedEnvelope = source.getBounds
  }
}
