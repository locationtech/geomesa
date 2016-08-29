/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import java.util.UUID

import com.googlecode.cqengine.attribute.Attribute
import com.googlecode.cqengine.index.hash.HashIndex
import com.googlecode.cqengine.index.radix.RadixTreeIndex
import com.googlecode.cqengine.index.unique.UniqueIndex
import com.googlecode.cqengine.{ConcurrentIndexedCollection, IndexedCollection}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.memory.cqengine.attribute.SimpleFeatureAttribute
import org.locationtech.geomesa.memory.cqengine.index.GeoIndex
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType.CQIndexType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.util.Try

// See geomesa/geomesa-utils/src/main/scala/org/locationtech/geomesa/utils/geotools/Conversions.scala
object CQIndexingOptions extends LazyLogging {
  def getCQIndexType(ad: AttributeDescriptor): CQIndexType = {
    Option(ad.getUserData.get(OPT_CQ_INDEX).asInstanceOf[String])
      .flatMap(c => Try(CQIndexType.withName(c)).toOption).getOrElse(CQIndexType.NONE)
  }

  def setCQIndexType(ad: AttributeDescriptor, indexType: CQIndexType) {
    ad.getUserData.put(OPT_CQ_INDEX, indexType.toString)
  }

  def buildIndexedCollection(sft: SimpleFeatureType): IndexedCollection[SimpleFeature] = {
    val cqcache: IndexedCollection[SimpleFeature] = new ConcurrentIndexedCollection[SimpleFeature]()

    // Add Geometry index on default geometry first.
    addGeoIndex(sft.getGeometryDescriptor, cqcache)

    // Add fid index
    // TODO: should we always build this or expose building this as an option?
    addFidIndex(cqcache)

    // Add other indexes
    sft.getAttributeDescriptors.foreach {
      addIndex(_, cqcache)
    }

    cqcache
  }

  def addIndex(ad: AttributeDescriptor, coll: IndexedCollection[SimpleFeature]): Unit = {
    getCQIndexType(ad) match {
      case CQIndexType.DEFAULT   =>
        ad.getType.getBinding match {
          // Comparable fields should have a Navigable Index
          case c if
            classOf[java.lang.Integer].isAssignableFrom(c) ||
            classOf[java.lang.Long].isAssignableFrom(c) ||
            classOf[java.lang.Float].isAssignableFrom(c) ||
            classOf[java.lang.Double].isAssignableFrom(c) ||
            classOf[java.util.Date].isAssignableFrom(c)            => addNavigableIndex(ad, coll)
          case c if classOf[java.lang.String].isAssignableFrom(c)  => addRadixIndex(ad, coll)
          case c if classOf[Geometry].isAssignableFrom(c)          => addGeoIndex(ad, coll)
          case c if classOf[UUID].isAssignableFrom(c)              => addUniqueIndex(ad, coll)
          // TODO: Decide how boolean fields should be indexed
          case c if classOf[java.lang.Boolean].isAssignableFrom(c) => addHashIndex(ad, coll)
        }

      case CQIndexType.NAVIGABLE => addNavigableIndex(ad, coll)
      case CQIndexType.RADIX     => addRadixIndex(ad, coll)
      case CQIndexType.UNIQUE    => addUniqueIndex(ad, coll)
      case CQIndexType.HASH      => addHashIndex(ad, coll)
      case CQIndexType.NONE      => // NO-OP
    }
  }

  def addFidIndex(coll: IndexedCollection[SimpleFeature]): Unit = {
    val attribute = SFTAttributes.fidAttribute
    coll.addIndex(HashIndex.onAttribute(attribute))
  }

  def addGeoIndex(ad: AttributeDescriptor, coll: IndexedCollection[SimpleFeature]): Unit = {
    // TODO: Add logic to allow for the geo-index to be disabled?  (Low priority)
    val defaultGeom: Attribute[SimpleFeature, Geometry] =
      new SimpleFeatureAttribute(classOf[Geometry], ad.getLocalName)
    coll.addIndex(GeoIndex.onAttribute(defaultGeom))
  }

  def addNavigableIndex(ad: AttributeDescriptor, coll: IndexedCollection[SimpleFeature]): Unit = {
    val binding = ad.getType.getBinding
    binding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntNavIndex(ad, coll)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongNavIndex(ad, coll)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatNavIndex(ad, coll)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleNavIndex(ad, coll)
      case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateNavIndex(ad, coll)
    }
  }

  def addRadixIndex(ad: AttributeDescriptor, coll: IndexedCollection[SimpleFeature]): Unit = {
    if (classOf[java.lang.String].isAssignableFrom(ad.getType.getBinding)) {
      val attribute: Attribute[SimpleFeature, String] =
        SFTAttributes.buildSimpleFeatureAttribute(ad).asInstanceOf[Attribute[SimpleFeature, String]]
      coll.addIndex(RadixTreeIndex.onAttribute(attribute))
    } else {
      logger.warn(s"Failed to add a Radix index for attribute ${ad.getLocalName}.")
    }
  }

  def addUniqueIndex(ad: AttributeDescriptor, coll: IndexedCollection[SimpleFeature]): Unit = {
    val attribute = SFTAttributes.buildSimpleFeatureAttribute(ad)
    coll.addIndex(UniqueIndex.onAttribute(attribute))
  }

  def addHashIndex(ad: AttributeDescriptor, coll: IndexedCollection[SimpleFeature]): Unit = {
    val attribute = SFTAttributes.buildSimpleFeatureAttribute(ad)
    coll.addIndex(HashIndex.onAttribute(attribute))
  }
}

object CQIndexType extends Enumeration {
  type CQIndexType = Value
  val DEFAULT   = Value("default")   // Let GeoMesa pick.
  val NAVIGABLE = Value("navigable") // Use for numeric fields and Date?
  val RADIX     = Value("radix")     // Use for strings

  val UNIQUE    = Value("unique")    // Use only for unique fields; could be string, Int, Long
  val HASH      = Value("hash")      // Use for 'enumerated' strings

  val NONE      = Value("none")
}
