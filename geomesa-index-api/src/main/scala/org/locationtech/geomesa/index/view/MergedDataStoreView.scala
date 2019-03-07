/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import java.io.IOException

import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureReader, SimpleFeatureSource}
import org.geotools.feature.{AttributeTypeBuilder, FeatureTypes, NameImpl}
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.index.view.MergedQueryRunner.MergedStats
import org.locationtech.geomesa.utils.geotools.{SchemaBuilder, SimpleFeatureTypes}
import org.opengis.feature.`type`.{GeometryDescriptor, Name}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
  * Merged querying against multiple data stores
  *
  * @param stores delegate stores
  * @param namespace namespace
  */
class MergedDataStoreView(val stores: Seq[(DataStore, Option[Filter])], namespace: Option[String] = None)
    extends HasGeoMesaStats with ReadOnlyDataStore {

  import scala.collection.JavaConverters._

  require(stores.nonEmpty, "No delegate stores configured")

  private [view] val runner = new MergedQueryRunner(this, stores)

  override val stats: GeoMesaStats = new MergedStats(stores)

  override def getTypeNames: Array[String] = stores.map(_._1.getTypeNames).reduceLeft(_ intersect _)

  override def getNames: java.util.List[Name] =
    java.util.Arrays.asList(getTypeNames.map(t => new NameImpl(namespace.orNull, t)): _*)

  override def getSchema(name: Name): SimpleFeatureType = getSchema(name.getLocalPart)

  override def getSchema(typeName: String): SimpleFeatureType = {
    val schemas = stores.map(_._1.getSchema(typeName))

    if (schemas.contains(null)) {
      return null
    }

    lazy val fail = new IOException("Delegate schemas do not match: " +
        schemas.map(SimpleFeatureTypes.encodeType).mkString(" :: "))

    schemas.reduceLeft[SimpleFeatureType] { case (left, right) =>
      if (left.getAttributeCount != right.getAttributeCount) {
        throw fail
      }
      val builder = new SchemaBuilder()
      val attribute = new AttributeTypeBuilder()

      val leftDescriptors = left.getAttributeDescriptors.iterator()
      val rightDescriptors = right.getAttributeDescriptors.iterator()

      while (leftDescriptors.hasNext) {
        val leftDescriptor = leftDescriptors.next
        val rightDescriptor = rightDescriptors.next
        if (leftDescriptor.getLocalName != rightDescriptor.getLocalName) {
          throw fail
        }
        val leftBinding = leftDescriptor.getType.getBinding
        val rightBinding = rightDescriptor.getType.getBinding
        // determine a common binding if possible, for things like java.sql.TimeStamp vs java.util.Date
        if (leftBinding == rightBinding || leftBinding.isAssignableFrom(rightBinding)) {
          attribute.binding(leftBinding)
        } else if (rightBinding.isAssignableFrom(leftBinding)) {
          attribute.binding(rightBinding)
        } else {
          throw fail
        }

        // add the user data from each descriptor so the delegate stores have it if needed
        leftDescriptor.getUserData.asScala.foreach { case (k, v) => attribute.userData(k, v) }
        rightDescriptor.getUserData.asScala.foreach { case (k, v) => attribute.userData(k, v) }

        Some(leftDescriptor).collect { case g: GeometryDescriptor => attribute.crs(g.getCoordinateReferenceSystem) }

        builder.addAttribute(attribute.buildDescriptor(leftDescriptor.getLocalName))
      }
      builder.build(namespace.orNull, typeName)
    }
  }

  override def getFeatureSource(name: Name): SimpleFeatureSource = getFeatureSource(name.getLocalPart)

  override def getFeatureSource(typeName: String): SimpleFeatureSource = {
    val sources = stores.map { case (store, filter) => (store.getFeatureSource(typeName), filter) }
    new MergedFeatureSourceView(this, sources, getSchema(typeName))
  }

  override def getFeatureReader(query: Query, transaction: Transaction): SimpleFeatureReader =
    GeoMesaFeatureReader(getSchema(query.getTypeName), query, runner, None, None)

  override def dispose(): Unit = stores.foreach(_._1.dispose())

  override def getInfo: ServiceInfo = {
    val info = new DefaultServiceInfo()
    info.setDescription(s"Features from ${getClass.getSimpleName}")
    info.setSchema(FeatureTypes.DEFAULT_NAMESPACE)
    info
  }

  override def getLockingManager: LockingManager = null
}
