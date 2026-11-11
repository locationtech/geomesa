/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.api.data._
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.feature.FeatureVisitor
import org.geotools.api.filter.Filter
import org.geotools.api.filter.sort.SortBy
import org.geotools.api.util.ProgressListener
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.filter.andFilters
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureCollection.GeoMesaFeatureVisitingCollection
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureSource.DelegatingResourceInfo
import org.locationtech.geomesa.index.process.GeoMesaProcessVisitor
import org.locationtech.geomesa.index.stats.HasGeoMesaStats

import java.awt.RenderingHints.Key
import java.util.Collections

/** Public owner-preserving facade. Each operation resolves one private merge plan. */
private[view] class RolloverFeatureSourceView(
    owner: RolloverDataStoreView,
    typeName: String,
    resolve: () => MergedDataStoreView,
    schema: () => SimpleFeatureType,
    capabilities: () => QueryCapabilities
  ) extends SimpleFeatureSource with HasGeoMesaStats {

  override val stats: RolloverStats = new RolloverStats(name => {
    owner.ensureOpen()
    require(name == typeName, s"Feature source '$typeName' cannot query '$name'")
    resolve()
  })

  override def getSchema: SimpleFeatureType = schema()
  override def getName: Name = getSchema.getName
  override def getDataStore: DataStore = owner
  override def getQueryCapabilities: QueryCapabilities = capabilities()
  override def getSupportedHints: java.util.Set[Key] = Collections.emptySet[Key]()
  override def getInfo: ResourceInfo = new DelegatingResourceInfo(this)
  override def addFeatureListener(listener: FeatureListener): Unit = throw new UnsupportedOperationException()
  override def removeFeatureListener(listener: FeatureListener): Unit = throw new UnsupportedOperationException()

  private def query(input: Query): Query = {
    owner.ensureOpen()
    val copy = new Query(input)
    require(copy.getTypeName == null || copy.getTypeName == typeName,
      s"Feature source '$typeName' cannot query '${copy.getTypeName}'")
    copy.setTypeName(typeName)
    copy
  }

  override def getCount(input: Query): Int = {
    val copy = query(input)
    resolve().getFeatureSource(typeName).getCount(copy)
  }

  override def getBounds: ReferencedEnvelope = getBounds(new Query(typeName))
  override def getBounds(input: Query): ReferencedEnvelope = {
    val copy = query(input)
    resolve().getFeatureSource(typeName).getBounds(copy)
  }

  override def getFeatures: SimpleFeatureCollection = getFeatures(new Query(typeName))
  override def getFeatures(filter: Filter): SimpleFeatureCollection = getFeatures(new Query(typeName, filter))
  override def getFeatures(input: Query): SimpleFeatureCollection = {
    val copy = query(input)
    val plan = resolve()
    new OperationCollection(plan, copy)
  }

  private class OperationCollection(plan: MergedDataStoreView, operation: Query)
      extends GeoMesaFeatureVisitingCollection(
        pinnedSource(plan), new RolloverStats(name => {
          require(name == typeName, "Operation feature type must not change")
          plan
        }), operation) {

    private val operationSource = pinnedSource(plan)
    private lazy val featureReader = plan.getFeatureReader(plan.getSchema(typeName), Transaction.AUTO_COMMIT, new Query(operation))

    override def getSchema: SimpleFeatureType = featureReader.schema
    override def reader(): FeatureReader[SimpleFeatureType, SimpleFeature] = {
      owner.ensureOpen()
      featureReader.reader()
    }
    override def getCount: Int = operationSource.getCount(operation)
    override def getBounds: ReferencedEnvelope = operationSource.getBounds(operation)
    override def size: Int = math.max(0, getCount)

    override def subCollection(filter: Filter): SimpleFeatureCollection = {
      val copy = new Query(operation)
      copy.setFilter(andFilters(Seq(copy.getFilter, filter)))
      new OperationCollection(plan, copy)
    }

    override def sort(order: SortBy): SimpleFeatureCollection = {
      val copy = new Query(operation)
      copy.setSortBy(Option(copy.getSortBy).fold(Array(order))(_ :+ order): _*)
      new OperationCollection(plan, copy)
    }

    override def accepts(visitor: FeatureVisitor, progress: ProgressListener): Unit = visitor match {
      // Do not expose the private plan or the collection's mutable Query to process visitors.
      case v: GeoMesaProcessVisitor => v.execute(operationSource, new Query(operation))
      case _ => super.accepts(visitor, progress)
    }
  }

  private def pinnedSource(plan: MergedDataStoreView): RolloverFeatureSourceView =
    new RolloverFeatureSourceView(owner, typeName, () => plan, () => plan.getSchema(typeName),
      () => plan.getFeatureSource(typeName).getQueryCapabilities)
}
