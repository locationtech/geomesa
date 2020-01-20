/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

import java.io.IOException

import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data.{DataStore, DefaultServiceInfo, LockingManager, Query, ServiceInfo, Transaction}
import org.geotools.feature.{AttributeTypeBuilder, FeatureTypes, NameImpl}
import org.locationtech.geomesa.utils.geotools.{SchemaBuilder, SimpleFeatureTypes}
import org.opengis.feature.`type`.{GeometryDescriptor, Name}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

package object view {

  import org.locationtech.geomesa.filter.andFilters

  /**
    * Helper method to merge a filtered data store query
    *
    * @param query query
    * @param filter data store filter
    * @return
    */
  def mergeFilter(query: Query, filter: Option[Filter]): Query = {
    filter match {
      case None => query
      case Some(f) =>
        val q = new Query(query)
        q.setFilter(if (q.getFilter == Filter.INCLUDE) { f } else { andFilters(Seq(q.getFilter, f)) })
        q
    }
  }

  /**
    * Helper method to merge a filtered data store query
    *
    * @param filter filter
    * @param option data store filter
    * @return
    */
  def mergeFilter(filter: Filter, option: Option[Filter]): Filter = {
    option match {
      case None => filter
      case Some(f) => if (filter == Filter.INCLUDE) { f } else { andFilters(Seq(filter, f)) }
    }
  }

  /**
    * Read only data store - does not support creating/updating/deleting schemas or features
    */
  trait ReadOnlyDataStore extends DataStore {

    private def error = throw new NotImplementedError("This data store is read-only")

    override def createSchema(featureType: SimpleFeatureType): Unit = error
    override def updateSchema(typeName: Name, featureType: SimpleFeatureType): Unit = error
    override def updateSchema(typeName: String, featureType: SimpleFeatureType): Unit = error
    override def removeSchema(typeName: Name): Unit = error
    override def removeSchema(typeName: String): Unit = error
    override def getFeatureWriter(typeName: String, transaction: Transaction): SimpleFeatureWriter = error
    override def getFeatureWriter(typeName: String, filter: Filter, transaction: Transaction): SimpleFeatureWriter = error
    override def getFeatureWriterAppend(typeName: String, transaction: Transaction): SimpleFeatureWriter = error
  }

  /**
    * Data store that merges the schemas from multiple delegate stores and presents them as a unified result
    *
    * @param stores delegate stores
    * @param namespace schema namespace
    */
  abstract class MergedDataStoreSchemas(stores: Seq[DataStore], namespace: Option[String])
      extends ReadOnlyDataStore {

    import scala.collection.JavaConverters._

    override def getTypeNames: Array[String] = stores.map(_.getTypeNames).reduceLeft(_ intersect _)

    override def getNames: java.util.List[Name] =
      java.util.Arrays.asList(getTypeNames.map(t => new NameImpl(namespace.orNull, t)): _*)

    override def getSchema(name: Name): SimpleFeatureType = getSchema(name.getLocalPart)

    override def getSchema(typeName: String): SimpleFeatureType = {
      val schemas = stores.map(_.getSchema(typeName))

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

    override def dispose(): Unit = stores.foreach(_.dispose())

    override def getInfo: ServiceInfo = {
      val info = new DefaultServiceInfo()
      info.setDescription(s"Features from ${getClass.getSimpleName}")
      info.setSchema(FeatureTypes.DEFAULT_NAMESPACE)
      info
    }

    override def getLockingManager: LockingManager = null
  }
}
