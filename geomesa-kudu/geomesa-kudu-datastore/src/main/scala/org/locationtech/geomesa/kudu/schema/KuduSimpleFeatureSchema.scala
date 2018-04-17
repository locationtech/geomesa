/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.schema

import org.apache.kudu.ColumnSchema
import org.apache.kudu.client.{KuduPredicate, RowResult}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.kudu.KuduValue
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema.{KuduDeserializer, KuduFilter}
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
class KuduSimpleFeatureSchema(sft: SimpleFeatureType) {

  import scala.collection.JavaConverters._

  private val adapters =
    sft.getAttributeDescriptors.asScala.map(KuduColumnAdapter(sft, _)).asInstanceOf[Seq[KuduColumnAdapter[AnyRef]]]

  /**
    * Columns for the simple feature type. Does not include primary key columns for the index (including
    * the feature ID column). Note that attributes may be encoded as multiple columns
    */
  val schema: Seq[ColumnSchema] = adapters.flatMap(_.columns)

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
    var i = -1
    adapters.map { adapter => i += 1; KuduValue(feature.getAttribute(i), adapter) }
  }

  /**
    * Deserializer for reading column values into simple features
    *
    * @return
    */
  def deserializer: KuduDeserializer = new KuduDeserializer(sft, adapters)

  /**
    * Deserializer for reading a subset of column values into simple features
    *
    * @param transform simple feature type containing a subset of attributes
    * @return
    */
  def deserializer(transform: SimpleFeatureType): KuduDeserializer = {
    // note: keep in same order as attributes
    val subset = transform.getAttributeDescriptors.asScala.map { d =>
      val name = d.getLocalName
      adapters.find(_.name == name).getOrElse {
        throw new IllegalArgumentException(s"Transform attribute '$name' does not correspond " +
            "to a field in the original schema")
      }
    }
    new KuduDeserializer(transform, subset)
  }
}

object KuduSimpleFeatureSchema {

  class KuduDeserializer(sft: SimpleFeatureType, adapters: Seq[KuduColumnAdapter[_ <: AnyRef]]) {

    private val withIndices = adapters.map(a => (a, sft.indexOf(a.name)))

    def deserialize(row: RowResult): SimpleFeature = deserialize(row, new ScalaSimpleFeature(sft, ""))

    def deserialize(row: RowResult, result: SimpleFeature): SimpleFeature = {
      withIndices.foreach { case (adapter, i) => result.setAttribute(i, adapter.readFromRow(row)) }
      result
    }
  }

  case class KuduFilter(predicates: Seq[KuduPredicate], filter: Option[Filter])
}
