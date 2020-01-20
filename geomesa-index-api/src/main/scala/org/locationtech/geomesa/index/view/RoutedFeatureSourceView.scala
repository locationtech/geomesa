/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import java.awt.RenderingHints.Key
import java.util.Collections

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureSource.DelegatingResourceInfo
import org.locationtech.geomesa.index.view.MergedFeatureSourceView.MergedQueryCapabilities
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
  * Feature source that routes queries to the appropriate store
  *
  * @param ds data store
  * @param sft simple feature type
  */
class RoutedFeatureSourceView(ds: RoutedDataStoreView, sft: SimpleFeatureType)
    extends SimpleFeatureSource with LazyLogging {

  lazy private val hints = Collections.unmodifiableSet(Collections.emptySet[Key])

  lazy private val capabilities =
    new MergedQueryCapabilities(ds.stores.map(_.getFeatureSource(sft.getTypeName).getQueryCapabilities))

  override def getSchema: SimpleFeatureType = sft

  override def getCount(query: Query): Int =
    ds.router.route(sft, query).map(_.getFeatureSource(sft.getTypeName).getCount(query)).getOrElse(0)

  override def getBounds: ReferencedEnvelope = getBounds(new Query(sft.getTypeName))

  override def getBounds(query: Query): ReferencedEnvelope = {
    ds.router.route(sft, query).map(_.getFeatureSource(sft.getTypeName).getBounds(query)).getOrElse {
      new ReferencedEnvelope(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
    }
  }

  override def getQueryCapabilities: QueryCapabilities = capabilities

  override def getFeatures: SimpleFeatureCollection = getFeatures(Filter.INCLUDE)

  override def getFeatures(filter: Filter): SimpleFeatureCollection =
    getFeatures(new Query(sft.getTypeName, filter))

  override def getFeatures(query: Query): SimpleFeatureCollection = {
    ds.router.route(sft, query) match {
      case None => new ListFeatureCollection(sft)
      case Some(store) => store.getFeatureSource(sft.getTypeName).getFeatures(query)
    }
  }

  override def getName: Name = getSchema.getName

  override def getDataStore: DataStore = ds

  override def getSupportedHints: java.util.Set[Key] = hints

  override def getInfo: ResourceInfo = new DelegatingResourceInfo(this)

  override def addFeatureListener(listener: FeatureListener): Unit = throw new NotImplementedError()

  override def removeFeatureListener(listener: FeatureListener): Unit = throw new NotImplementedError()
}
