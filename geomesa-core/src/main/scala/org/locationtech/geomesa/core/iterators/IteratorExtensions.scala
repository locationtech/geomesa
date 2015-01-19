/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.iterators

import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.iterators.IteratorExtensions.OptionMap
import org.locationtech.geomesa.core.transform.TransformCreator
import org.locationtech.geomesa.feature.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.feature.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.feature.{FeatureEncoding, SimpleFeatureDecoder, SimpleFeatureEncoder}
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
    featureType = SimpleFeatureTypes.createType(sftName,
      options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE))
    featureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
  }
}

/**
 * Provides an index value decoder
 */
trait HasIndexValueDecoder extends IteratorExtensions {

  var indexEncoder: IndexValueEncoder = null

  // index value encoder/decoder
  abstract override def init(featureType: SimpleFeatureType, options: OptionMap) = {
    super.init(featureType, options)
    val indexValues = SimpleFeatureTypes.createType(featureType.getTypeName,
      options.get(GEOMESA_ITERATORS_SFT_INDEX_VALUE))
    indexEncoder = IndexValueEncoder(indexValues)
  }
}

/**
 * Provides a feature builder and a method to create a feature from an index value
 */
trait HasFeatureBuilder extends HasFeatureType {

  import org.locationtech.geomesa.core.iterators.IteratorTrigger._

  private var featureBuilder: SimpleFeatureBuilder = null

  private lazy val geomIdx = featureType.indexOf(featureType.getGeometryDescriptor.getLocalName)
  private lazy val sdtgIdx = featureType.startTimeName.map(featureType.indexOf).getOrElse(-1)
  private lazy val edtgIdx = featureType.endTimeName.map(featureType.indexOf).getOrElse(-1)
  private lazy val hasDtg  = sdtgIdx != -1 || edtgIdx != -1

  private lazy val attrArray = Array.ofDim[AnyRef](featureType.getAttributeCount)

  override def initFeatureType(options: OptionMap) = {
    super.initFeatureType(options)
    featureBuilder = ScalaSimpleFeatureFactory.featureBuilder(featureType)
  }

  def encodeIndexValueToSF(value: DecodedIndexValue): SimpleFeature = {
    // Build and fill the Feature. This offers some performance gain over building and then setting the attributes.
    featureBuilder.buildFeature(value.id, attributeArray(featureBuilder.getFeatureType, value))
  }

  /**
   * Construct and fill an array of the SimpleFeature's attribute values
   */
  def attributeArray(sft: SimpleFeatureType, indexValue: DecodedIndexValue): Array[AnyRef] = {
    val attrArray = new Array[AnyRef](sft.getAttributeCount)
    indexValue.attributes.foreach { case (name, value) =>
      val index = sft.indexOf(name)
      if (index != -1) {
        attrArray.update(index, value.asInstanceOf[AnyRef])
      }
    }
    attrArray
  }

  /**
   * Construct and fill an array of the SimpleFeature's attribute values
   * Reuse attrArray as it is copied inside of feature builder anyway
   */
  private def fillAttributeArray(geomValue: Geometry, date: Option[Date]) = {
    // always set the mandatory geo element
    attrArray(geomIdx) = geomValue
    // if dtgDT exists, attempt to fill the elements corresponding to the start and/or end times
    date.foreach { time =>
      if (sdtgIdx != -1) attrArray(sdtgIdx) = time
      if (edtgIdx != -1) attrArray(edtgIdx) = time
    }
  }
}

/**
 * Provides a feature encoder and decoder
 */
trait HasFeatureDecoder extends IteratorExtensions {

  var featureDecoder: SimpleFeatureDecoder = null
  var featureEncoder: SimpleFeatureEncoder = null
  val defaultEncoding = org.locationtech.geomesa.core.data.DEFAULT_ENCODING.toString

  // feature encoder/decoder
  abstract override def init(featureType: SimpleFeatureType, options: OptionMap) = {
    super.init(featureType, options)
    // this encoder is for the source sft
    val encodingOpt = Option(options.get(FEATURE_ENCODING)).getOrElse(defaultEncoding)
    featureDecoder = SimpleFeatureDecoder(featureType, encodingOpt)
    featureEncoder = SimpleFeatureEncoder(featureType, encodingOpt)
  }
}

/**
 * Provides a spatio-temporal filter (date and geometry only) if the iterator config specifies one
 */
trait HasSpatioTemporalFilter extends IteratorExtensions {

  private var filterOption: Option[Filter] = None
  private var dateAttributeIndex: Option[Int] = None
  private var testSimpleFeature: Option[SimpleFeature] = None

  lazy val stFilter: Option[(Geometry, Option[Long]) => Boolean] =
    for (filter <- filterOption.filterNot(_ == Filter.INCLUDE); feat <- testSimpleFeature) yield {
      (geom: Geometry, olong: Option[Long]) => {
        feat.setDefaultGeometry(geom)
        dateAttributeIndex.foreach { i =>
          olong.map(new Date(_)).foreach(feat.setAttribute(i, _))
        }
        filter.evaluate(feat)
      }
    }

  // spatio-temporal filter config
  abstract override def init(featureType: SimpleFeatureType, options: OptionMap) = {
    super.init(featureType, options)
    dateAttributeIndex = getDtgFieldName(featureType).map(featureType.indexOf)
    if (options.containsKey(ST_FILTER_PROPERTY_NAME)) {
      val filterString = options.get(ST_FILTER_PROPERTY_NAME)
      filterOption = Some(ECQL.toFilter(filterString))
      val sfb = new SimpleFeatureBuilder(featureType)
      testSimpleFeature = Some(sfb.buildFeature("test"))
    }
  }
}

/**
 * Provides an arbitrary filter if the iterator config specifies one
 */
trait HasEcqlFilter extends IteratorExtensions {

  private var filterOption: Option[Filter] = None

  lazy val ecqlFilter: Option[(SimpleFeature) => Boolean] =
    filterOption.filterNot(_ == Filter.INCLUDE).map { filter =>
      (sf: SimpleFeature) => filter.evaluate(sf)
    }

  // other filter config
  abstract override def init(featureType: SimpleFeatureType, options: OptionMap) = {
    super.init(featureType, options)
    if (options.containsKey(GEOMESA_ITERATORS_ECQL_FILTER)) {
      val filterString = options.get(GEOMESA_ITERATORS_ECQL_FILTER)
      filterOption = Some(ECQL.toFilter(filterString))
    }
  }
}

/**
 * Provides a feature type transformation if the iterator config specifies one
 */
trait HasTransforms extends IteratorExtensions {

  import org.locationtech.geomesa.core.data.DEFAULT_ENCODING

  private var targetFeatureType: Option[SimpleFeatureType] = None
  private var transformString: Option[String] = None
  private var transformEncoding: FeatureEncoding = null

  lazy val transform: Option[(SimpleFeature) => Array[Byte]] =
    for { featureType <- targetFeatureType; string <- transformString } yield {
      val transform = TransformCreator.createTransform(featureType, transformEncoding, string)
      (sf: SimpleFeature) => transform(sf)
    }

  // feature type transforms
  abstract override def init(featureType: SimpleFeatureType, options: OptionMap) = {
    super.init(featureType, options)
    if (options.containsKey(GEOMESA_ITERATORS_TRANSFORM_SCHEMA)) {
      val transformSchema = options.get(GEOMESA_ITERATORS_TRANSFORM_SCHEMA)

      targetFeatureType = Some(SimpleFeatureTypes.createType(this.getClass.getCanonicalName, transformSchema))
      targetFeatureType.foreach(_.decodeUserData(options, GEOMESA_ITERATORS_TRANSFORM_SCHEMA))

      transformString = Option(options.get(GEOMESA_ITERATORS_TRANSFORM))
      transformEncoding = Option(options.get(FEATURE_ENCODING)).map(FeatureEncoding.withName(_))
          .getOrElse(DEFAULT_ENCODING)
    }
  }
}

/**
 * Provides deduplication if the iterator config specifies it
 */
trait HasInMemoryDeduplication extends IteratorExtensions {

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
  lazy val checkUniqueId: Option[(String) => Boolean] =
    Some(deduplicate).filter(_ == true).map { _ =>
      (id: String) =>
        Option(id)
          .filter(_ => inMemoryIdCache.size < maxInMemoryIdCacheEntries)
          .forall(inMemoryIdCache.add(_))
    }

  abstract override def init(featureType: SimpleFeatureType, options: OptionMap) = {
    super.init(featureType, options)
    // check for dedupe - we don't need to dedupe for density queries
    if (!options.containsKey(GEOMESA_ITERATORS_IS_DENSITY_TYPE)) {
      deduplicate = IndexSchema.mayContainDuplicates(featureType)
      if (deduplicate) {
        if (options.containsKey(DEFAULT_CACHE_SIZE_NAME)) {
          maxInMemoryIdCacheEntries = options.get(DEFAULT_CACHE_SIZE_NAME).toInt
        }
        inMemoryIdCache = new java.util.HashSet[String](maxInMemoryIdCacheEntries)
      }
    }
  }
}