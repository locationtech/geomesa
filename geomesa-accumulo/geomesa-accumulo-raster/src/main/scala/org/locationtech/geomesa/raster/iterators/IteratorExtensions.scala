/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.accumulo.index.IndexValueEncoder.IndexValueEncoderImpl
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features._
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.raster.iterators.IteratorExtensions._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
 * Defines common iterator functionality in traits that can be mixed-in to iterator implementations
 */
trait IteratorExtensions {
  def init(featureType: SimpleFeatureType, options: OptionMap)
}

object IteratorExtensions {

  type OptionMap = java.util.Map[String, String]

  val ST_FILTER_PROPERTY_NAME = "geomesa.index.filter"
  val DEFAULT_CACHE_SIZE_NAME = "geomesa.index.cache-size"
  val FEATURE_ENCODING        = "geomesa.feature.encoding"

  val GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE      = "geomesa.iterators.aggregator-types"
  val GEOMESA_ITERATORS_SFT_NAME                 = "geomesa.iterators.sft-name"
  val GEOMESA_ITERATORS_SFT_INDEX_VALUE          = "geomesa.iterators.sft.index-value-schema"
  val GEOMESA_ITERATORS_ECQL_FILTER              = "geomesa.iterators.ecql-filter"
  val GEOMESA_ITERATORS_TRANSFORM                = "geomesa.iterators.transform"
  val GEOMESA_ITERATORS_TRANSFORM_SCHEMA         = "geomesa.iterators.transform.schema"
  val GEOMESA_ITERATORS_IS_DENSITY_TYPE          = "geomesa.iterators.is-density-type"

  val USER_DATA = ".userdata."

  /**
    *  Copy UserData entries taken from a SimpleFeatureType into an IteratorSetting for later transfer back into
    *  a SimpleFeatureType
    *
    *  This works around the fact that SimpleFeatureTypes.encodeType ignores the UserData
    *
    */
  def encodeUserData(cfg: IteratorSetting, userData: java.util.Map[AnyRef,AnyRef], keyPrefix: String): Unit = {
    import scala.collection.JavaConversions._
    val fullPrefix = keyPrefix + USER_DATA
    userData.foreach { case (k, v) => cfg.addOption(fullPrefix + k.toString, v.toString)}
  }

  /**
    *  Copy UserData entries taken from an IteratorSetting/Options back into
    *  a SimpleFeatureType
    *
    *  This works around the fact that SimpleFeatureTypes.encodeType ignores the UserData
    *
    */
  def decodeUserData(sft: SimpleFeatureType, options: java.util.Map[String,String], keyPrefix:String): Unit = {
    import scala.collection.JavaConversions._
    val fullPrefix = keyPrefix + USER_DATA
    val ud = options.collect { case (k, v) if k.startsWith(fullPrefix) => k.stripPrefix(fullPrefix) -> v }
    sft.getUserData.putAll(ud)
  }
}

/**
 * We need a concrete class to mix the traits into. This way they can share a common 'init' method
 * that will be called for each trait. See http://stackoverflow.com/a/1836619
 */
class HasIteratorExtensions extends IteratorExtensions {
  override def init(featureType: SimpleFeatureType, options: OptionMap) = {}
}

/**
 * Provides a feature type based on the iterator config
 */
trait HasFeatureType {

  var featureType: SimpleFeatureType = null

  // feature type config
  def initFeatureType(options: OptionMap) = {
    val sftName = Option(options.get(GEOMESA_ITERATORS_SFT_NAME)).getOrElse(this.getClass.getSimpleName)
    featureType = SimpleFeatureTypes.createType(sftName, options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE))
    decodeUserData(featureType, options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
  }
}

/**
 * Provides an index value decoder
 */
trait HasIndexValueDecoder extends IteratorExtensions {

  var indexSft: SimpleFeatureType = null
  var indexEncoder: SimpleFeatureSerializer = null

  // index value encoder/decoder
  abstract override def init(featureType: SimpleFeatureType, options: OptionMap) = {
    super.init(featureType, options)
    indexSft = SimpleFeatureTypes.createType(featureType.getTypeName,
      options.get(GEOMESA_ITERATORS_SFT_INDEX_VALUE))
    indexEncoder = new IndexValueEncoderImpl(featureType)
  }
}

/**
 * Provides a feature encoder and decoder
 */
trait HasFeatureDecoder extends IteratorExtensions {

  var featureDecoder: SimpleFeatureSerializer = null
  var featureEncoder: SimpleFeatureSerializer = null
  val defaultEncoding = SerializationType.KRYO

  // feature encoder/decoder
  abstract override def init(featureType: SimpleFeatureType, options: OptionMap) = {
    super.init(featureType, options)
    // this encoder is for the source sft
    val encoding = Option(options.get(FEATURE_ENCODING)).map(SerializationType.withName).getOrElse(defaultEncoding)
    featureDecoder = SimpleFeatureDeserializers(featureType, encoding)
    featureEncoder = SimpleFeatureSerializers(featureType, encoding)
  }
}

/**
 * Provides a spatio-temporal filter (date and geometry only) if the iterator config specifies one
 */
trait HasSpatioTemporalFilter extends IteratorExtensions {

  import IteratorExtensions.ST_FILTER_PROPERTY_NAME

  var stFilter: Filter = null

  // spatio-temporal filter config
  abstract override def init(featureType: SimpleFeatureType, options: OptionMap) = {
    super.init(featureType, options)
    if (options.containsKey(ST_FILTER_PROPERTY_NAME)) {
      val filter = FastFilterFactory.toFilter(featureType, options.get(ST_FILTER_PROPERTY_NAME))
      if (filter != Filter.INCLUDE) {
        stFilter = filter
      }
    }
  }
}

/**
 * Provides an arbitrary filter if the iterator config specifies one
 */
trait HasFilter extends IteratorExtensions {

  var filter: Filter = null

  // other filter config
  abstract override def init(featureType: SimpleFeatureType, options: OptionMap) = {
    super.init(featureType, options)
    if (options.containsKey(GEOMESA_ITERATORS_ECQL_FILTER)) {
      val ecql = FastFilterFactory.toFilter(featureType, options.get(GEOMESA_ITERATORS_ECQL_FILTER))
      if (ecql != Filter.INCLUDE) {
        filter = ecql
      }
    }
  }
}

/**
 * Provides a feature type transformation if the iterator config specifies one
 */
trait HasTransforms extends IteratorExtensions {

  type TransformFunction = (SimpleFeature) => Array[Byte]
  var transform: TransformFunction = null

  // feature type transforms
  abstract override def init(featureType: SimpleFeatureType, options: OptionMap) = {
    super.init(featureType, options)
    if (options.containsKey(GEOMESA_ITERATORS_TRANSFORM_SCHEMA) &&
        options.containsKey(GEOMESA_ITERATORS_TRANSFORM)) {
      val transformSchema = options.get(GEOMESA_ITERATORS_TRANSFORM_SCHEMA)
      val targetFeatureType = SimpleFeatureTypes.createType(this.getClass.getCanonicalName, transformSchema)
      decodeUserData(targetFeatureType, options, GEOMESA_ITERATORS_TRANSFORM_SCHEMA)

      val transformString = options.get(GEOMESA_ITERATORS_TRANSFORM)
      val transformEncoding = Option(options.get(FEATURE_ENCODING)).map(SerializationType.withName)
          .getOrElse(SerializationType.KRYO)

      transform = TransformCreator.createTransform(targetFeatureType, transformEncoding, transformString)
    }
  }
}

/**
 * Provides deduplication if the iterator config specifies it
 */
trait HasInMemoryDeduplication extends IteratorExtensions {

  import IteratorExtensions.DEFAULT_CACHE_SIZE_NAME

  type CheckUniqueId = (String) => Boolean

  private var deduplicate: Boolean = false

  // each thread maintains its own (imperfect!) list of the unique identifiers it has seen
  private var maxInMemoryIdCacheEntries = 10000
  private var inMemoryIdCache: java.util.HashSet[String] = null

  /**
   * Returns a local estimate as to whether the current identifier
   * is likely to be a duplicate.
   *
   * Because we set a limit on how many unique IDs will be preserved in
   * the local cache, a TRUE response is always accurate, but a FALSE
   * response may not be accurate.  (That is, this cache allows for false-
   * negatives, but no false-positives.)  We accept this, because there is
   * a final, client-side filter that will eliminate all duplicate IDs
   * definitively.  The purpose of the local cache is to reduce traffic
   * through the remainder of the iterator/aggregator pipeline as quickly as
   * possible.
   *
   * @return False if this identifier is in the local cache; True otherwise
   */
  var checkUniqueId: CheckUniqueId = null

  abstract override def init(featureType: SimpleFeatureType, options: OptionMap) = {
    super.init(featureType, options)
    // check for dedupe - we don't need to dedupe for density queries
    if (!options.containsKey(GEOMESA_ITERATORS_IS_DENSITY_TYPE)) {
      deduplicate = featureType.nonPoints
      if (deduplicate) {
        if (options.containsKey(DEFAULT_CACHE_SIZE_NAME)) {
          maxInMemoryIdCacheEntries = options.get(DEFAULT_CACHE_SIZE_NAME).toInt
        }
        inMemoryIdCache = new java.util.HashSet[String](maxInMemoryIdCacheEntries)
        checkUniqueId =
            (id: String) => if (inMemoryIdCache.size < maxInMemoryIdCacheEntries) {
              inMemoryIdCache.add(id)
            } else {
              !inMemoryIdCache.contains(id)
            }
      }
    }
  }
}

object TransformCreator {

  /**
    * Create a function to transform a feature from one sft to another...this will
    * result in a new feature instance being created and encoded.
    *
    * The function returned may NOT be ThreadSafe to due the fact it contains a
    * SimpleFeatureEncoder instance which is not thread safe to optimize performance
    */
  def createTransform(targetFeatureType: SimpleFeatureType,
                      featureEncoding: SerializationType,
                      transformString: String): (SimpleFeature => Array[Byte]) = {
    import scala.collection.JavaConversions._

    val encoder = SimpleFeatureSerializers(targetFeatureType, featureEncoding)
    val defs = TransformProcess.toDefinition(transformString)

    val newSf = new ScalaSimpleFeature(targetFeatureType, "reusable")

    (feature: SimpleFeature) => {
      newSf.setId(feature.getIdentifier.getID)
      defs.foreach { t => newSf.setAttribute(t.name, t.expression.evaluate(feature)) }
      encoder.serialize(newSf)
    }
  }
}
