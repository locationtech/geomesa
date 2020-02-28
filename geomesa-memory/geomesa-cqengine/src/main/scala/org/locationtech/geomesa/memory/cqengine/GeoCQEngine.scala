/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine

import java.util
import java.util.UUID

import com.googlecode.cqengine.attribute.Attribute
import com.googlecode.cqengine.index.hash.HashIndex
import com.googlecode.cqengine.index.navigable.NavigableIndex
import com.googlecode.cqengine.index.radix.RadixTreeIndex
import com.googlecode.cqengine.index.unique.UniqueIndex
import com.googlecode.cqengine.query.option.DeduplicationStrategy
import com.googlecode.cqengine.query.simple.{All, Equal}
import com.googlecode.cqengine.query.{Query, QueryFactory}
import com.googlecode.cqengine.{ConcurrentIndexedCollection, IndexedCollection}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.memory.cqengine.attribute.SimpleFeatureAttribute
import org.locationtech.geomesa.memory.cqengine.index.GeoIndexType
import org.locationtech.geomesa.memory.cqengine.index.param.{BucketIndexParam, GeoIndexParams}
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType.CQIndexType
import org.locationtech.geomesa.memory.cqengine.utils._
import org.locationtech.geomesa.utils.index.SimpleFeatureIndex
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._

import scala.collection.JavaConversions._

class GeoCQEngine(val sft: SimpleFeatureType,
                  attributes: Seq[(String, CQIndexType)],
                  enableFidIndex: Boolean = false,
                  geoIndexType: GeoIndexType = GeoIndexType.Bucket,
                  geoIndexParam: Option[_ <: GeoIndexParams] = Option.empty,
                  dedupe: Boolean = true) extends SimpleFeatureIndex with LazyLogging {

  protected val cqcache: IndexedCollection[SimpleFeature] = new ConcurrentIndexedCollection[SimpleFeature]()

  def this(sft: SimpleFeatureType,
           attributes: Seq[(String, CQIndexType)],
           enableFidIndex: Boolean,
           geomResolution: (Int, Int),
           dedupe: Boolean) = {
    this(sft, attributes, enableFidIndex, GeoIndexType.Bucket, Option.apply(new BucketIndexParam(geomResolution._1, geomResolution._2).asInstanceOf[GeoIndexParams]), dedupe)
  }


  def this(sft: SimpleFeatureType,
           attributes: Seq[(String, CQIndexType)],
           enableFidIndex: Boolean,
           geomResolution: (Int, Int)) = {
    this(sft, attributes, enableFidIndex, geomResolution, true)
  }

  addIndices()

  // methods from SimpleFeatureIndex

  override def insert(feature: SimpleFeature): Unit = cqcache.add(feature)

  override def insert(features: Iterable[SimpleFeature]): Unit = cqcache.addAll(features)

  override def update(feature: SimpleFeature): SimpleFeature = {
    val existing = remove(feature.getID)
    cqcache.add(feature)
    existing
  }

  override def remove(id: String): SimpleFeature = {
    val existing = get(id)
    if (existing != null) {
      cqcache.remove(existing)
    }
    existing
  }

  override def get(id: String): SimpleFeature = {
    // if this gets used, set enableFidIndex=true
    cqcache.retrieve(new Equal(SFTAttributes.fidAttribute, id)).headOption.orNull
  }

  override def query(filter: Filter): Iterator[SimpleFeature] = {
    val query = filter.accept(new CQEngineQueryVisitor(sft), null).asInstanceOf[Query[SimpleFeature]]
    val iter = if (dedupe) {
      val dedupeOpt = QueryFactory.deduplicate(DeduplicationStrategy.LOGICAL_ELIMINATION)
      cqcache.retrieve(query, QueryFactory.queryOptions(dedupeOpt)).iterator()
    } else {
      cqcache.retrieve(query).iterator()
    }
    if (query.isInstanceOf[All[_]]) { iter.filter(filter.evaluate) } else { iter }
  }

  def size(): Int = cqcache.size()
  def clear(): Unit = cqcache.clear()

  private def addIndices(): Unit = {

    import CQIndexType._

    if (enableFidIndex) {
      cqcache.addIndex(HashIndex.onAttribute(SFTAttributes.fidAttribute))
    }

    // track attribute names in case there are duplicates...
    val names = scala.collection.mutable.Set.empty[String]

    // works around casting to T <: Comparable[T]
    def navigableIndex[T <: Comparable[T]](name: String): NavigableIndex[T, SimpleFeature] = {
      val attribute = new SimpleFeatureAttribute(classOf[Comparable[_]], sft, name)
      NavigableIndex.onAttribute(attribute.asInstanceOf[Attribute[SimpleFeature, T]])
    }

    attributes.foreach { case (name, indexType) =>
      if (indexType != NONE && names.add(name)) {
        val descriptor = sft.getDescriptor(name)
        require(descriptor != null, s"Could not find descriptor for $name in schema ${sft.getTypeName}")
        val binding = descriptor.getType.getBinding
        val index = indexType match {
          case RADIX | DEFAULT if classOf[String].isAssignableFrom(binding) =>
              RadixTreeIndex.onAttribute(new SimpleFeatureAttribute(classOf[String], sft, name))

          case GEOMETRY | DEFAULT if classOf[Geometry].isAssignableFrom(binding) =>
              val attribute = new SimpleFeatureAttribute(binding.asInstanceOf[Class[Geometry]], sft, name)
              GeoIndexFactory.onAttribute(sft, attribute, geoIndexType, geoIndexParam);

          case DEFAULT if classOf[UUID].isAssignableFrom(binding) =>
              UniqueIndex.onAttribute(new SimpleFeatureAttribute(classOf[UUID], sft, name))

          case DEFAULT if classOf[java.lang.Boolean].isAssignableFrom(binding) =>
              HashIndex.onAttribute(new SimpleFeatureAttribute(classOf[java.lang.Boolean], sft, name))

          case NAVIGABLE | DEFAULT if classOf[Comparable[_]].isAssignableFrom(binding) =>
            navigableIndex(name)

          case UNIQUE =>
            UniqueIndex.onAttribute(new SimpleFeatureAttribute(classOf[AnyRef], sft, name))

          case HASH =>
            HashIndex.onAttribute(new SimpleFeatureAttribute(classOf[AnyRef], sft, name))

          case t =>
              throw new IllegalArgumentException(s"No CQEngine binding available for type $t and class $binding")
        }
        cqcache.addIndex(index)
      }
    }
  }
}
