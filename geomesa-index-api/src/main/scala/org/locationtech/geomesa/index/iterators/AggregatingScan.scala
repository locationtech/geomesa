/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.TransformSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

trait AggregatingScan[T <: AnyRef { def isEmpty: Boolean; def clear(): Unit }]
    extends SamplingIterator with ConfiguredScan with LazyLogging {

  import AggregatingScan.Configuration._

  private var sft: SimpleFeatureType = _
  private var transformSft: SimpleFeatureType = _
  // note: index won't have a hook to the data store, so some operations aren't available
  private var index: GeoMesaFeatureIndex[_, _] = _

  private var validate: SimpleFeature => Boolean = _

  // our accumulated result
  private var result: T = _

  private var reusableSf: KryoBufferSimpleFeature = _
  private var reusableTransformSf: TransformSimpleFeature = _
  private var hasTransform: Boolean = _

  override def init(options: Map[String, String]): Unit = {
    val spec = options(SftOpt)
    sft = IteratorCache.sft(spec)
    index = options.get(IndexSftOpt) match {
      case None => IteratorCache.index(sft, spec, options(IndexOpt))
      case Some(ispec) => IteratorCache.index(IteratorCache.sft(ispec), ispec, options(IndexOpt))
    }

    // noinspection ScalaDeprecation
    val kryo = if (index.serializedWithId) { SerializationOptions.none } else { SerializationOptions.withoutId }
    reusableSf = IteratorCache.serializer(spec, kryo).getReusableFeature
    reusableSf.setIdParser(index.getIdFromRow(_, _, _, null))

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
      case (None, None)             => _ => true
      case (Some(filt), None)       => filt.evaluate(_)
      case (None, Some(samp))       => samp.apply
      case (Some(filt), Some(samp)) => f => filt.evaluate(f) && samp.apply(f)
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
      try {
        nextData(setValues)
        if (validateFeature(reusableSf)) {
          // write the record to our aggregated results
          if (hasTransform) {
            aggregateResult(reusableTransformSf, result)
          } else {
            aggregateResult(reusableSf, result)
          }
        }
      } catch {
        case NonFatal(e) => logger.error("Error aggregating value:", e)
      }
    }
    // noinspection LanguageFeature
    if (result.isEmpty) { null } else {
      encodeResult(result)
    }
  }

  private def setValues(row: Array[Byte], rowOffset: Int, rowLength: Int,
                        value: Array[Byte], valueOffset: Int, valueLength: Int): Unit = {
    reusableSf.setIdBuffer(row, rowOffset, rowLength)
    reusableSf.setBuffer(value, valueOffset, valueLength)
  }

  // returns true if there is more data to read
  def hasNextData: Boolean
  // seValues should be invoked with the underlying data
  def nextData(setValues: (Array[Byte], Int, Int, Array[Byte], Int, Int) => Unit): Unit

  // validate that we should aggregate this feature
  // if overridden, ensure call to super.validateFeature
  protected def validateFeature(f: SimpleFeature): Boolean = validate(f)

  // hook to allow result to be chunked up
  // note: it's important to return false occasionally, so that we can check for cancelled scans
  protected def notFull(result: T): Boolean

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
    val IndexSftOpt        = "index-sft"
    val CqlOpt             = "cql"
    val TransformSchemaOpt = "tsft"
    val TransformDefsOpt   = "tdefs"
  }

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                transform: Option[(String, SimpleFeatureType)],
                sample: Option[(Float, Option[String])]): Map[String, String] = {
    import Configuration._
    val indexSftOpt = Some(index.sft).collect {
      case s if s != sft => SimpleFeatureTypes.encodeType(s, includeUserData = true)
    }
    sample.map(SamplingIterator.configure(sft, _)).getOrElse(Map.empty) ++ optionalMap(
      SftOpt             -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
      IndexOpt           -> index.identifier,
      IndexSftOpt        -> indexSftOpt,
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