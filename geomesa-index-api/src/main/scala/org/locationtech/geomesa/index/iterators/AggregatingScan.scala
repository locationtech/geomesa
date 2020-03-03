/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataUtilities
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.TransformSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.AggregatingScan.{AggregateCallback, CqlSampleValidator, CqlValidator, RowValidator, RowValue, SampleValidator, ValidateAll}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.Try
import scala.util.control.NonFatal

trait AggregatingScan[T <: AggregatingScan.Result] extends SamplingIterator with ConfiguredScan with LazyLogging {

  import AggregatingScan.Configuration._

  private var validate: RowValidator = _

  // our accumulated result
  private var result: T = _

  private var batchSize: Int = _

  private var reusableSf: KryoBufferSimpleFeature = _
  private var aggregateSf: SimpleFeature = _

  override def init(options: Map[String, String]): Unit = {
    val spec = options(SftOpt)
    val sft = IteratorCache.sft(spec)
    // note: index won't have a hook to the data store, so some operations aren't available
    val index = options.get(IndexSftOpt) match {
      case None => IteratorCache.index(sft, spec, options(IndexOpt))
      case Some(ispec) => IteratorCache.index(IteratorCache.sft(ispec), ispec, options(IndexOpt))
    }

    // noinspection ScalaDeprecation
    val kryo = if (index.serializedWithId) { SerializationOptions.none } else { SerializationOptions.withoutId }
    reusableSf = IteratorCache.serializer(spec, kryo).getReusableFeature
    reusableSf.setIdParser(index.getIdFromRow(_, _, _, null))
    aggregateSf = reusableSf

    val transformSft = options.get(TransformDefsOpt).map { t =>
      val ts = options.getOrElse(TransformSchemaOpt,
        throw new IllegalArgumentException("Defined a transform but no transform schema"))
      val transformSft = IteratorCache.sft(ts)
      val reusableTransformSf = TransformSimpleFeature(IteratorCache.sft(spec), transformSft, t)
      reusableTransformSf.setFeature(reusableSf)
      aggregateSf = reusableTransformSf // note: side-effect in map
      transformSft
    }
    val sampling = sample(options)
    val cql = options.get(CqlOpt).map(IteratorCache.filter(sft, spec, _))
    validate = (cql, sampling) match {
      case (None, None)             => ValidateAll
      case (Some(filt), None)       => new CqlValidator(filt)
      case (None, Some(samp))       => new SampleValidator(samp)
      case (Some(filt), Some(samp)) => new CqlSampleValidator(filt,  samp)
    }
    batchSize = options.get(BatchSizeOpt).map(_.toInt).getOrElse(defaultBatchSize)
    result = createResult(sft, transformSft, batchSize, options)
  }

  /**
   * Aggregates a batch of data. May not exhaust the underlying data
   *
   * @param callback callback to provide for results
   * @return callback
   */
  def aggregate[A <: AggregateCallback](callback: A): A = {
    // noinspection LanguageFeature
    result.init()

    val status = new AggregateStatus(callback)

    var rowValue = try { if (hasNextData) { nextData() } else { null } } catch {
      case NonFatal(e) => logger.error("Error in underlying scan while aggregating value:", e); null
    }

    while (rowValue != null) {
      try {
        reusableSf.setIdBuffer(rowValue.row, rowValue.rowOffset, rowValue.rowLength)
        reusableSf.setBuffer(rowValue.value, rowValue.valueOffset, rowValue.valueLength)
        if (validate(reusableSf)) {
          // write the record to our aggregated results
          status.aggregated(result.aggregate(aggregateSf))
        } else {
          status.skipped()
        }
      } catch {
        case NonFatal(e) =>
          logger.error(s"Error aggregating value for ${debugSf()}:", e)
          status.skipped()
      }

      rowValue = try { if (status.continue() && hasNextData) { nextData() } else { null } } catch {
        case NonFatal(e) => logger.error("Error in underlying scan while aggregating value:", e); null
      }
    }

    status.done()

    callback
  }

  private def debugSf(): String = Try(DataUtilities.encodeFeature(aggregateSf)).getOrElse(s"$aggregateSf")

  // returns true if there is more data to read
  protected def hasNextData: Boolean
  // returns the next row of data
  protected def nextData(): RowValue

  // default batch size
  protected def defaultBatchSize: Int

  /**
   * Create the result object for the current scan
   *
   * @param sft simple feature type
   * @param transform transform, if any
   * @param batchSize batch size
   * @param options scan options
   * @return
   */
  protected def createResult(
      sft: SimpleFeatureType,
      transform: Option[SimpleFeatureType],
      batchSize: Int,
      options: Map[String, String]): T

  /**
   * Class for tracking status of current aggregation
   *
   * @param callback results callback
   */
  private class AggregateStatus(callback: AggregateCallback) {

    private var count = 0
    private var skip = 0

    def aggregated(count: Int): Unit = this.count += count

    def skipped(): Unit = skip += 1

    // noinspection LanguageFeature
    def continue(): Boolean = {
      if (count >= batchSize) {
        callback.batch(bytes())
      } else if (skip >= batchSize) {
        skip = 0
        callback.partial(bytes())
      } else {
        true
      }
    }

    def done(): Unit = {
      if (count > 0) {
        callback.batch(bytes())
      }
      result.cleanup()
    }

    // noinspection LanguageFeature
    private def bytes(): Array[Byte] = {
      count = 0
      skip = 0
      result.encode()
    }
  }
}

object AggregatingScan {

  /**
   * Aggregation result
   */
  trait Result {

    /**
     * Initialize the result for a scan
     */
    def init(): Unit

    /**
     * Aggregate a feature. May be called anytime after `init`
     *
     * @param sf simple feature
     * @return number of entries aggregated
     */
    def aggregate(sf: SimpleFeature): Int

    /**
     * Encode current aggregation and reset the result. May be called anytime after `init`
     */
    def encode(): Array[Byte]

    /**
     * Dispose of any resources used by the scan. If the result is re-used, `init` will be called
     * again before anything else
     */
    def cleanup(): Unit
  }

  // configuration keys
  object Configuration {
    val SftOpt             = "sft"
    val IndexOpt           = "index"
    val IndexSftOpt        = "index-sft"
    val CqlOpt             = "cql"
    val TransformSchemaOpt = "tsft"
    val TransformDefsOpt   = "tdefs"
    val BatchSizeOpt       = "batch"
  }

  def configure(
      sft: SimpleFeatureType,
      index: GeoMesaFeatureIndex[_, _],
      filter: Option[Filter],
      transform: Option[(String, SimpleFeatureType)],
      sample: Option[(Float, Option[String])],
      batchSize: Int): Map[String, String] = {

    val indexSftOpt = Some(index.sft).collect {
      case s if s != sft => SimpleFeatureTypes.encodeType(s, includeUserData = true)
    }
    sample.map(SamplingIterator.configure(sft, _)).getOrElse(Map.empty) ++ optionalMap(
      Configuration.SftOpt             -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
      Configuration.IndexOpt           -> index.identifier,
      Configuration.IndexSftOpt        -> indexSftOpt,
      Configuration.CqlOpt             -> filter.map(ECQL.toCQL),
      Configuration.TransformDefsOpt   -> transform.map(_._1),
      Configuration.TransformSchemaOpt -> transform.map(t => SimpleFeatureTypes.encodeType(t._2)),
      Configuration.BatchSizeOpt       -> batchSize.toString
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

  case class RowValue(
      row: Array[Byte],
      rowOffset: Int,
      rowLength: Int,
      value: Array[Byte],
      valueOffset: Int,
      valueLength: Int
    )

  /**
   * Callback for handling partial results, so that a scan can be interrupted if it's taking too long
   */
  trait AggregateCallback {

    /**
     * Invoked when a batch of data has been aggregated
     *
     * @param bytes aggregated bytes
     * @return true to continue scanning, false to stop
     */
    def batch(bytes: Array[Byte]): Boolean

    /**
     * Invoked when a partial batch of data has been aggregated, but a batch's worth of data
     * has been skipped over
     *
     * @param bytes partially aggregated bytes, lazily evaluated. if the results are not accessed (i.e. lazy
     *              statement remains unevaluated), they will be included in the next batch or partial batch
     * @return true to continue scanning, false to stop
     */
    def partial(bytes: => Array[Byte]): Boolean
  }

  private sealed trait RowValidator {
    def apply(sf: SimpleFeature): Boolean
  }

  private case object ValidateAll extends RowValidator {
    override def apply(sf: SimpleFeature): Boolean = true
  }

  private class CqlValidator(filter: Filter) extends RowValidator {
    override def apply(sf: SimpleFeature): Boolean = filter.evaluate(sf)
  }

  private class SampleValidator(sample: SimpleFeature => Boolean) extends RowValidator {
    override def apply(sf: SimpleFeature): Boolean = sample(sf)
  }

  private class CqlSampleValidator(filter: Filter, sample: SimpleFeature => Boolean) extends RowValidator {
    override def apply(sf: SimpleFeature): Boolean = filter.evaluate(sf) && sample(sf)
  }
}
