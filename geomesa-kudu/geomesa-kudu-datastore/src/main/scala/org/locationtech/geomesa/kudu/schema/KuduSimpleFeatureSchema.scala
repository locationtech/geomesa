/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.schema

import java.util.concurrent.ConcurrentHashMap

import org.apache.kudu.ColumnSchema
import org.apache.kudu.client.KuduPredicate
import org.locationtech.geomesa.kudu.KuduValue
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema.KuduFilter
import org.locationtech.geomesa.utils.cache.CacheKeyGenerator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Handles conversions between Kudu table columns and simple feature attributes.
  *
  * Note: does not handle feature ID, as this is generally part of the row primary key and handled
  * by the individual index implementation
  *
  * @param sft simple feature type
  */
class KuduSimpleFeatureSchema private (sft: SimpleFeatureType) {

  import scala.collection.JavaConverters._

  val adapters: IndexedSeq[KuduColumnAdapter[AnyRef]] = {
    val builder = IndexedSeq.newBuilder[KuduColumnAdapter[AnyRef]]
    builder.sizeHint(sft.getAttributeCount)
    sft.getAttributeDescriptors.asScala.foreach { d =>
      builder += KuduColumnAdapter(sft, d).asInstanceOf[KuduColumnAdapter[AnyRef]]
    }
    builder.result()
  }

  /**
    * Columns for the simple feature type. Does not include primary key columns for the index (including
    * the feature ID column). Note that attributes may be encoded as multiple columns
    */
  val schema: Seq[ColumnSchema] = adapters.flatMap(_.columns)

  /**
    * Write columns for the simple feature type. Includes all the columns in `schema`, plus any additional
    * columns that are used for push-down predicates, but not actually returned from queries.
    */
  val writeSchema: Seq[ColumnSchema] = adapters.flatMap(_.writeColumns)

  /**
    * Columns for a subset of attributes. An attribute may be encoded as multiple columns
    *
    * @param attributes simple feature attributes
    * @return
    */
  def schema(attributes: Seq[String]): Seq[ColumnSchema] =
    adapters.filter(a => attributes.contains(a.name)).flatMap(_.columns)

  /**
    * Create push-down predicates from an ECQL filter
    *
    * @param filter filter
    * @return
    */
  def predicate(filter: Filter): KuduFilter =
    adapters.foldLeft(KuduFilter(Seq.empty, Some(filter))) { case (kf, adapter) =>
      kf.filter match {
        case None => kf // the entire filter has been dealt with
        case Some(f) =>
          val p = adapter.predicate(f)
          p.copy(predicates = p.predicates ++ kf.predicates)
      }
    }

  /**
    * Convert a simple feature into a series of column values
    *
    * @param feature simple feature
    * @return
    */
  def serialize(feature: SimpleFeature): Seq[KuduValue[_]] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce
    val builder = Seq.newBuilder[KuduValue[_]]
    builder.sizeHint(adapters.length)
    adapters.foreachIndex { case (adapter, i) => builder += KuduValue(feature.getAttribute(i), adapter) }
    builder.result
  }
}

object KuduSimpleFeatureSchema {

  private val schemas = new ConcurrentHashMap[String, KuduSimpleFeatureSchema]()

  def apply(sft: SimpleFeatureType): KuduSimpleFeatureSchema = {
    val key = CacheKeyGenerator.cacheKey(sft)
    var schema = schemas.get(key)
    if (schema == null) {
      schema = new KuduSimpleFeatureSchema(sft)
      schemas.put(key, schema)
    }
    schema
  }

  case class KuduFilter(predicates: Seq[KuduPredicate], filter: Option[Filter])
}
