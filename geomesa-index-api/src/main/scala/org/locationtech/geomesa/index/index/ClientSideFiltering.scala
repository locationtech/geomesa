/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.process.vector.TransformProcess
import org.geotools.process.vector.TransformProcess.Definition
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.{KryoFeatureSerializer, ProjectingKryoFeatureDeserializer}
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.PropertyName

/**
  * Takes fully-serialized simple features and applies filters and transforms as appropriate
  *
  * @tparam R result entry coming back from database
  */
trait ClientSideFiltering[R] {

  this: GeoMesaFeatureIndex[_, _, _] =>

  /**
    * Get row and value bytes from a scan result
    *
    * @param result result
    * @return
    */
  def rowAndValue(result: R): RowAndValue

  /**
    * Turns scan results into simple features
    *
    * @param sft simple feature type
    * @param ecql filter to apply
    * @param transform transform definitions and return simple feature type
    * @return
    */
  def resultsToFeatures(sft: SimpleFeatureType,
                        ecql: Option[Filter],
                        transform: Option[(String, SimpleFeatureType)]): Iterator[R] => Iterator[SimpleFeature] = {
    import scala.collection.JavaConverters._

    val tdefs = transform.map { case (defs, schema) =>
      val definitions = TransformProcess.toDefinition(defs).asScala.toArray
      val indices = definitions.map(_.expression).map {
        case p: PropertyName => sft.indexOf(p.getPropertyName)
        case _ => -1
      }
      (definitions, indices, schema)
    }

    (ecql, tdefs) match {
      case (Some(e), None)               =>
        val fn = toFeaturesWithFilter(sft, e)
        (iter) => iter.flatMap { r => fn(r) }

      case (Some(e), Some((t, i, tsft))) =>
        val fn = toFeaturesWithFilterTransform(sft, e, t, i,  tsft)
        (iter) => iter.flatMap { r => fn(r) }

      case (None, Some((t, i, tsft)))    =>
        val fn = toFeaturesWithTransform(sft, t, i, tsft)
        (iter) => iter.map { r => fn(r) }

      case (None, None)                  =>
        val fn = toFeaturesDirect(sft)
        (iter) => iter.map { r => fn(r) }
    }
  }

  /**
    * Converts results to features directly
    *
    * @param sft simple feature type
    * @return
    */
  def toFeaturesDirect(sft: SimpleFeatureType): (R) => SimpleFeature = {
    val getId = getIdFromRow(sft)
    val deserializer = KryoFeatureSerializer(sft, SerializationOptions.withoutId)
    (result) => {
      val RowAndValue(row, rowOffset, rowLength, value, valueOffset, valueLength) = rowAndValue(result)
      val sf = deserializer.deserialize(value, valueOffset, valueLength)
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row, rowOffset, rowLength, sf))
      sf
    }
  }

  /**
    * Converts results into features and filters by ecql
    *
    * @param sft simple feature type
    * @param ecql filter
    * @return
    */
  def toFeaturesWithFilter(sft: SimpleFeatureType, ecql: Filter): (R) => Option[SimpleFeature] = {
    val getId = getIdFromRow(sft)
    val deserializer = KryoFeatureSerializer(sft, SerializationOptions.withoutId)
    (result) => {
      val RowAndValue(row, rowOffset, rowLength, value, valueOffset, valueLength) = rowAndValue(result)
      val sf = deserializer.deserialize(value, valueOffset, valueLength)
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row, rowOffset, rowLength, sf))
      Some(sf).filter(ecql.evaluate)
    }
  }

  /**
    * Converts results into features according to a transform
    *
    * @param sft simple feature type
    * @param transforms transform definitions
    * @param indices indices of the attributes being projected, or -1 if a transform isn't a simple projection
    * @param transformSft type for the transformed simple feature
    * @return
    */
  def toFeaturesWithTransform(sft: SimpleFeatureType,
                              transforms: Array[Definition],
                              indices: Array[Int],
                              transformSft: SimpleFeatureType): (R) => SimpleFeature = {
    val getId = getIdFromRow(sft)
    if (indices.contains(-1)) {
      // need to evaluate the expressions against the original feature
      val reusableSf = KryoFeatureSerializer(sft, SerializationOptions.withoutId).getReusableFeature
      (result) => {
        val RowAndValue(row, rowOffset, rowLength, value, valueOffset, valueLength) = rowAndValue(result)
        reusableSf.setBuffer(value, valueOffset, valueLength)
        val id = getId(row, rowOffset, rowLength, reusableSf)
        reusableSf.setId(id)
        val values = transforms.map(_.expression.evaluate(reusableSf))
        new ScalaSimpleFeature(transformSft, id, values)
      }
    } else {
      // we can do a projecting deserialization
      val deserializer = new ProjectingKryoFeatureDeserializer(sft, transformSft, SerializationOptions.withoutId)
      (result) => {
        val RowAndValue(row, rowOffset, rowLength, value, valueOffset, valueLength) = rowAndValue(result)
        val sf = deserializer.deserialize(value, valueOffset, valueLength)
        sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row, rowOffset, rowLength, sf))
        sf
      }
    }
  }

  /**
    * Converts results into the original sft, applies a filter, then applies a transform
    *
    * @param sft simple feature type
    * @param ecql filter
    * @param transforms transform definitions
    * @param indices indices of the attributes being projected, or -1 if a transform isn't a simple projection
    * @param transformSft type for the transformed simple feature
    * @return
    */
  def toFeaturesWithFilterTransform(sft: SimpleFeatureType,
                                    ecql: Filter,
                                    transforms: Array[Definition],
                                    indices: Array[Int],
                                    transformSft: SimpleFeatureType): (R) => Option[SimpleFeature] = {
    val getId = getIdFromRow(sft)
    if (indices.contains(-1)) {
      // need to evaluate the expressions against the original feature
      // can filter at the same time
      val deserializer = KryoFeatureSerializer(sft, SerializationOptions.withoutId)
      (result) => {
        val RowAndValue(row, rowOffset, rowLength, value, valueOffset, valueLength) = rowAndValue(result)
        val sf = deserializer.deserialize(value, valueOffset, valueLength)
        val id = getId(row, rowOffset, rowLength, sf)
        sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(id)
        if (ecql.evaluate(sf)) {
          val values = transforms.map(_.expression.evaluate(sf))
          Some(new ScalaSimpleFeature(transformSft, id, values))
        } else {
          None
        }
      }
    } else {
      // we can project attributes, but we have to deserialize the full feature to filter
      val reusableSf = KryoFeatureSerializer(sft, SerializationOptions.withoutId).getReusableFeature
      (result) => {
        val RowAndValue(row, rowOffset, rowLength, value, valueOffset, valueLength) = rowAndValue(result)
        reusableSf.setBuffer(value, valueOffset, valueLength)
        val id = getId(row, rowOffset, rowLength, reusableSf)
        reusableSf.setId(id)
        if (ecql.evaluate(reusableSf)) {
          val values = indices.map(reusableSf.getAttribute)
          Some(new ScalaSimpleFeature(transformSft, id, values))
        } else {
          None
        }
      }
    }
  }
}

object ClientSideFiltering {
  case class RowAndValue(row: Array[Byte], rowOffset: Int, rowLength: Int,
                         value: Array[Byte], valueOffset: Int, valueLength: Int)
}
