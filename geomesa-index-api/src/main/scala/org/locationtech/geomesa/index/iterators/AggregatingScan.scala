/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.TransformSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, GeoMesaIndexManager}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

trait AggregatingScan[T <: AnyRef { def isEmpty: Boolean; def clear(): Unit }]
    extends SamplingIterator with ConfiguredScan {

  import AggregatingScan.Configuration._

  protected def manager: GeoMesaIndexManager[_, _, _]

  private var sft: SimpleFeatureType = _
  private var transformSft: SimpleFeatureType = _
  private var index: GeoMesaFeatureIndex[_, _, _] = _

  private var validate: (SimpleFeature) => Boolean = _

  // our accumulated result
  private var result: T = _

  private var reusableSf: KryoBufferSimpleFeature = _
  private var reusableTransformSf: TransformSimpleFeature = _
  private var getId: (Array[Byte], Int, Int, SimpleFeature) => String = _
  private var hasTransform: Boolean = _

  override def init(options: Map[String, String]): Unit = {
    val spec = options(SftOpt)
    sft = IteratorCache.sft(spec)

    index = try { manager.index(options(IndexOpt)) } catch {
      case NonFatal(_) => throw new RuntimeException(s"Index option not configured correctly: ${options.get(IndexOpt)}")
    }

    // noinspection ScalaDeprecation
    if (index.serializedWithId) {
      reusableSf = IteratorCache.serializer(spec, SerializationOptions.none).getReusableFeature
      getId = (_, _, _, _) => reusableSf.getID
    } else {
      reusableSf = IteratorCache.serializer(spec, SerializationOptions.withoutId).getReusableFeature
      getId = index.getIdFromRow(sft)
    }

    val transform = options.get(TransformDefsOpt)
    val transformSchema = options.get(TransformSchemaOpt)
    for { t <- transform; ts <- transformSchema } {
      transformSft = IteratorCache.sft(ts)
      reusableTransformSf = TransformSimpleFeature(IteratorCache.sft(spec), transformSft, t)
      reusableTransformSf.setFeature(reusableSf)
    }
    hasTransform = transform.isDefined

    val sampling = sample(options)
    val cql = options.get(CqlOpt).map(IteratorCache.filter(sft, spec, _))
    validate = (cql, sampling) match {
      case (None, None)             => (_) => true
      case (Some(filt), None)       => filt.evaluate(_)
      case (None, Some(samp))       => samp.apply
      case (Some(filt), Some(samp)) => (f) => filt.evaluate(f) && samp.apply(f)
    }
    result = initResult(sft, if (hasTransform) { Some(transformSft) } else { None }, options)
  }

  /**
    * Aggregates a batch of data. May not exhaust the underlying data
    *
    * @return encoded aggregate batch, or null if no results
    */
  def aggregate(): Array[Byte] = {
    // noinspection LanguageFeature
    result.clear()
    while (hasNextData && notFull(result)) {
      nextData(setValues)
      if (validateFeature(reusableSf)) {
        // write the record to our aggregated results
        if (hasTransform) {
          aggregateResult(reusableTransformSf, result)
        } else {
          aggregateResult(reusableSf, result)
        }
      }
    }
    // noinspection LanguageFeature
    if (result.isEmpty) { null } else {
      encodeResult(result)
    }
  }

  private def setValues(row: Array[Byte], rowOffset: Int, rowLength: Int,
                        value: Array[Byte], valueOffset: Int, valueLength: Int): Unit = {
    reusableSf.setBuffer(value, valueOffset, valueLength)
    reusableSf.setId(getId(row, rowOffset, rowLength, reusableSf))
  }

  // returns true if there is more data to read
  def hasNextData: Boolean
  // seValues should be invoked with the underlying data
  def nextData(setValues: (Array[Byte], Int, Int, Array[Byte], Int, Int) => Unit): Unit

  // validate that we should aggregate this feature
  // if overridden, ensure call to super.validateFeature
  protected def validateFeature(f: SimpleFeature): Boolean = validate(f)

  // hook to allow result to be chunked up
  protected def notFull(result: T): Boolean = true

  // create the result object for the current scan
  protected def initResult(sft: SimpleFeatureType, transform: Option[SimpleFeatureType], options: Map[String, String]): T

  // add the feature to the current aggregated result
  protected def aggregateResult(sf: SimpleFeature, result: T): Unit

  // encode the result as a byte array
  protected def encodeResult(result: T): Array[Byte]
}

object AggregatingScan {

  // configuration keys
  object Configuration {
    val SftOpt             = "sft"
    val IndexOpt           = "index"
    val CqlOpt             = "cql"
    val TransformSchemaOpt = "tsft"
    val TransformDefsOpt   = "tdefs"
  }

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _, _],
                filter: Option[Filter],
                transform: Option[(String, SimpleFeatureType)],
                sample: Option[(Float, Option[String])]): Map[String, String] = {
    import Configuration._
    sample.map(SamplingIterator.configure(sft, _)).getOrElse(Map.empty) ++ optionalMap(
      SftOpt             -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
      IndexOpt           -> index.identifier,
      CqlOpt             -> filter.map(ECQL.toCQL),
      TransformDefsOpt   -> transform.map(_._1),
      TransformSchemaOpt -> transform.map(t => SimpleFeatureTypes.encodeType(t._2))
    )
  }

  def optionalMap(config: (String, Either[String, Option[String]])*): Map[String, String] =
    config.collect {
      case (k, Left(v))        => (k, v)
      case (k, Right(Some(v))) => (k, v)
    }.toMap

  // noinspection LanguageFeature
  implicit def StringToConfig(s: String): Either[String, Option[String]] = Left(s)
  // noinspection LanguageFeature
  implicit def OptionToConfig(s: Option[String]): Either[String, Option[String]] = Right(s)
}