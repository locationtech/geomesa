/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.schema

import org.apache.kudu.client.RowResult
import org.geotools.data.DataUtilities
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.features.{ScalaSimpleFeature, TransformSimpleFeature}
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.kudu.schema.KuduIndexColumnAdapter.FeatureIdAdapter
import org.locationtech.geomesa.kudu.schema.KuduSimpleFeatureSchema.KuduDeserializer
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.{Expression, PropertyName}

object KuduResultAdapter {

  /**
    * Turns scan results into simple features
    *
    * @param sft simple feature type
    * @param ecql filter to apply
    * @param transform transform definitions and return simple feature type
    * @return
    */
  def resultsToFeatures(sft: SimpleFeatureType,
                        schema: KuduSimpleFeatureSchema,
                        ecql: Option[Filter],
                        transform: Option[(String, SimpleFeatureType)]): ResultAdapter = {
    (transform, ecql) match {
      case (None, None) => toFeatures(sft, schema)
      case (None, Some(f)) => toFeaturesWithFilter(sft, schema, f)
      case (Some((tdefs, tsft)), None) => toFeaturesWithTransform(sft, schema, tsft, tdefs)
      case (Some((tdefs, tsft)), Some(f)) => toFeaturesWithFilterTransform(sft, schema, tsft, tdefs, f)
    }
  }

  private def toFeatures(sft: SimpleFeatureType, schema: KuduSimpleFeatureSchema): ResultAdapter = {
    val cols = schema.schema.map(_.getName).+:(FeatureIdAdapter.name)
    val adapter = toFeatures(schema.deserializer, new ScalaSimpleFeature(sft, "")) _
    ResultAdapter(cols, adapter)
  }

  private def toFeaturesWithFilter(sft: SimpleFeatureType,
                                   schema: KuduSimpleFeatureSchema,
                                   filter: Filter): ResultAdapter = {
    val cols = schema.schema.map(_.getName).+:(FeatureIdAdapter.name)
    val adapter = toFeaturesWithFilter(schema.deserializer, filter, new ScalaSimpleFeature(sft, "")) _
    ResultAdapter(cols, adapter)
  }

  private def toFeaturesWithTransform(sft: SimpleFeatureType,
                                      schema: KuduSimpleFeatureSchema,
                                      tsft: SimpleFeatureType,
                                      tdefs: String): ResultAdapter = {
    import scala.collection.JavaConverters._

    // determine all the attributes that we need to be able to evaluate the transform
    val attributes = TransformProcess.toDefinition(tdefs).asScala.map(_.expression).flatMap {
      case p: PropertyName => Seq(p.getPropertyName)
      case e: Expression   => DataUtilities.attributeNames(e, sft)
    }.distinct

    val subType = DataUtilities.createSubType(sft, attributes.toArray)
    subType.getUserData.putAll(sft.getUserData)

    val feature = new ScalaSimpleFeature(subType, "")
    val transformFeature = TransformSimpleFeature(subType, tsft, tdefs)
    transformFeature.setFeature(feature)

    val cols = schema.schema(attributes).map(_.getName).+:(FeatureIdAdapter.name)
    val deserializer = schema.deserializer(subType)

    val adapter = toFeaturesWithTransform(deserializer, feature, transformFeature) _
    ResultAdapter(cols, adapter)
  }

  private def toFeaturesWithFilterTransform(sft: SimpleFeatureType,
                                            schema: KuduSimpleFeatureSchema,
                                            tsft: SimpleFeatureType,
                                            tdefs: String,
                                            filter: Filter): ResultAdapter = {
    import scala.collection.JavaConverters._

    // determine all the attributes that we need to be able to evaluate the transform and filter
    val attributes = {
      val fromTransform = TransformProcess.toDefinition(tdefs).asScala.map(_.expression).flatMap {
        case p: PropertyName => Seq(p.getPropertyName)
        case e: Expression   => DataUtilities.attributeNames(e, sft)
      }
      val fromFilter = FilterHelper.propertyNames(filter, sft)
      (fromTransform ++ fromFilter).distinct
    }

    val subType = DataUtilities.createSubType(sft, attributes.toArray)
    subType.getUserData.putAll(sft.getUserData)

    val feature = new ScalaSimpleFeature(subType, "")
    val transformFeature = TransformSimpleFeature(subType, tsft, tdefs)
    transformFeature.setFeature(feature)

    val cols = schema.schema(attributes).map(_.getName).+:(FeatureIdAdapter.name)
    val deserializer = schema.deserializer(subType)

    val adapter = toFeaturesWithFilterTransform(deserializer, filter, feature, transformFeature) _
    ResultAdapter(cols, adapter)
  }

  /**
    * Converts results to features directly
    *
    * @param deserializer deserializer
    * @param feature reusable feature
    * @param results result iterator
    * @return
    */
  private def toFeatures(deserializer: KuduDeserializer,
                         feature: ScalaSimpleFeature)
                        (results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
    results.map { row =>
      feature.setId(FeatureIdAdapter.readFromRow(row))
      deserializer.deserialize(row, feature)
      feature
    }
  }

  /**
    * Converts results into features and filters by ecql
    *
    * @param deserializer deserializer
    * @param ecql filter
    * @param feature reusable feature
    * @param results result iterator
    * @return
    */
  private def toFeaturesWithFilter(deserializer: KuduDeserializer,
                                   ecql: Filter,
                                   feature: ScalaSimpleFeature)
                                  (results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
    results.flatMap { row =>
      feature.setId(FeatureIdAdapter.readFromRow(row))
      deserializer.deserialize(row, feature)
      if (ecql.evaluate(feature)) {
        Iterator.single(feature)
      } else {
        CloseableIterator.empty
      }
    }
  }

  private def toFeaturesWithTransform(deserializer: KuduDeserializer,
                                      feature: ScalaSimpleFeature,
                                      transformFeature: TransformSimpleFeature)
                                     (results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
    results.map { row =>
      feature.setId(FeatureIdAdapter.readFromRow(row))
      deserializer.deserialize(row, feature)
      transformFeature
    }
  }

  private def toFeaturesWithFilterTransform(deserializer: KuduDeserializer,
                                            ecql: Filter,
                                            feature: ScalaSimpleFeature,
                                            transformFeature: TransformSimpleFeature)
                                           (results: CloseableIterator[RowResult]): CloseableIterator[SimpleFeature] = {
    results.flatMap { row =>
      feature.setId(FeatureIdAdapter.readFromRow(row))
      deserializer.deserialize(row, feature)
      if (ecql.evaluate(feature)) {
        Iterator.single(transformFeature)
      } else {
        CloseableIterator.empty
      }
    }
  }

  case class ResultAdapter(columns: Seq[String],
                           adapter: CloseableIterator[RowResult] => CloseableIterator[SimpleFeature])
}
