/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.iterators


//import collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet
import org.joda.time.DateTime
import scala.util.Try

//import com.typesafe.scalalogging.slf4j.{Logger, Logging}
import com.vividsolutions.jts.geom._
import geomesa.core.data._
import geomesa.core.index._
import geomesa.utils.text.WKTUtils
import java.util.Date
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.data.{Query, DataUtilities}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.FilterAttributeExtractor
import org.geotools.filter.text.ecql.ECQL
import org.geotools.process.vector.TransformProcess
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import scala.Some


/**
 * This is an Index Only Iterator, to be used in situations where the data records are
 * not useful enough to pay the penalty of decoding when using the
 * SpatioTemporalIntersectingIterator.
 *
 * This iterator returns as its nextKey the key for the index. nextValue is
 * the value for the INDEX, mapped into a SimpleFeature
 *
 * Note that this extends the SpatioTemporalIntersectingIterator, but never creates a dataSource
 * and hence never iterates through it.
 */
class IndexIterator extends SpatioTemporalIntersectingIterator with SortedKeyValueIterator[Key, Value] {

  import geomesa.core._

  var featureBuilder: SimpleFeatureBuilder = null
  var featureEncoder: SimpleFeatureEncoder = null

  var outputAttributes: List[AttributeDescriptor] = null

  var indexAttributes: List[AttributeDescriptor] = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) {
    logger.debug("Transform requests index attributes only. Ignoring SimpleFeatures and using index information only.")
    logger.trace("Initializing classLoader")
    IndexIterator.initClassLoader(logger)

    val simpleFeatureTypeSpec = options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    val simpleFeatureType = DataUtilities.createType(this.getClass.getCanonicalName, simpleFeatureTypeSpec)

    // default to text if not found for backwards compatibility
    val encodingOpt = Option(options.get(FEATURE_ENCODING)).getOrElse(FeatureEncoding.TEXT.toString)
    featureEncoder = SimpleFeatureEncoderFactory.createEncoder(encodingOpt)

    featureBuilder = new SimpleFeatureBuilder(simpleFeatureType)

    val schemaEncoding = options.get(DEFAULT_SCHEMA_NAME)
    decoder = IndexSchema.getIndexEntryDecoder(schemaEncoding)

    if (options.containsKey(DEFAULT_POLY_PROPERTY_NAME)) {
      val polyWKT = options.get(DEFAULT_POLY_PROPERTY_NAME)
      poly = WKTUtils.read(polyWKT)
    }
    if (options.containsKey(DEFAULT_INTERVAL_PROPERTY_NAME))
      interval = IndexIterator.decodeInterval(
        options.get(DEFAULT_INTERVAL_PROPERTY_NAME))

    if (options.containsKey(DEFAULT_CACHE_SIZE_NAME))
      maxInMemoryIdCacheEntries = options.get(DEFAULT_CACHE_SIZE_NAME).toInt
    deduplicate = IndexSchema.mayContainDuplicates(simpleFeatureType)

    this.indexSource = source.deepCopy(env)
  }

  /**
   * Generates from the key's value a SimpleFeature that matches the current
   * (top) reference of the index-iterator.
   *
   * We emit the top-key from the index-iterator, and the top-value from the
   * converted key value.  This is *IMPORTANT*, as otherwise we do not emit rows
   * that honor the SortedKeyValueIterator expectation, and Bad Things Happen.
   */
  override def seekData(decodedValue: IndexSchema.DecodedIndexValue) {
    // now increment the value of nextKey, copy because reusing it is UNSAFE
    nextKey = new Key(indexSource.getTopKey)
    // using the already decoded index value, generate a SimpleFeature and set as the Value
    val nextSimpleFeature = IndexIterator.encodeIndexValueToSF(featureBuilder, decodedValue.id,
      decodedValue.geom, decodedValue.dtgMillis)
    nextValue = featureEncoder.encode(nextSimpleFeature)
  }

  override def deepCopy(env: IteratorEnvironment) =
    throw new UnsupportedOperationException("IndexIterator does not support deepCopy.")
}

object IndexIteratorTrigger {
  /**
   * Scans the ECQL predicate, the transform definition and transform schema to determine if only index attributes are
   * used/requested, and thus the IndexIterator can be used
   *
   */
  def useIndexOnlyIterator(ecqlPredicate: Option[String], query: Query) = {
    val transformDefs = Option(query.getHints.get(TRANSFORMS)).map(_.asInstanceOf[String])
    val transformSchema = Option(query.getHints.get(TRANSFORM_SCHEMA)).map(_.asInstanceOf[SimpleFeatureType])
    (ecqlPredicate, transformDefs, transformSchema) match {
      case (Some(ep), Some(td), Some(ts)) => isTransformToIndexOnly(td, ts) && filterOnIndexAttributes(ep, indexSFT)
      case (None, Some(td), Some(ts)) => isTransformToIndexOnly(td, ts)
      case _ => false
    }
  }

  def useTransformedSimpleFeatureType(ecqlPredicate: Option[String], query: Query) = {
    val transformDefs = Option(query.getHints.get(TRANSFORMS)).map(_.asInstanceOf[String])
    val transformSchema = Option(query.getHints.get(TRANSFORM_SCHEMA)).map(_.asInstanceOf[SimpleFeatureType])
    (ecqlPredicate, transformDefs, transformSchema) match {
      // there is still an ECQL predicate to apply, SimpleFeatureFilteringIterator still needed
      case (Some(ep), Some(td), Some(ts)) => passThroughFilter(ep) && isIdentityTransformation(td)
      // there is no ECQL predicate to apply, check if IndexIterator can produce the finalSimpleFeature
      case (None, Some(td), Some(ts)) => isIdentityTransformation(td)
      case _ => false
    }
  }

  /**
   * Checks the transform for mapping to the index attributes: geometry and optionally time
   */
  def isTransformToIndexOnly(transformDefs: String, transformSchema: SimpleFeatureType): Boolean = {
    isJustIndexAttributes(transformSchema, indexSFT)
  }

  /**
   * Checks to see if the transform schema references only variables present in the index schema
   */
  def isJustIndexAttributes(transformSchema: SimpleFeatureType, indexSchema: SimpleFeatureType): Boolean = {
    val transformDescriptorNames = transformSchema.getAttributeDescriptors.asScala.map {
      _.getLocalName
    }
    val indexDescriptorNames = indexSchema.getAttributeDescriptors.asScala.map {
      _.getLocalName
    }
    // while matching descriptors themselves are not always equal, their names are
    transformDescriptorNames.forall(indexDescriptorNames contains)
  }

  /**
   * Tests if a transform simply selects attributes, with no scaling or renaming
   */
  def isIdentityTransformation(transformDefs: String) = {
    // convert to a transform
    val theDefinitions = TransformProcess.toDefinition(transformDefs).asScala
    // check that, for each definition, the name and expression match
    theDefinitions.forall(aDef => aDef.name == aDef.expression.toString)
  }

  /**
   * Tests if the filter is applied to only attributes found in the target schema
   */
  def filterOnIndexAttributes(ecql_text: String, targetSchema: SimpleFeatureType): Boolean = {
    // convert the ECQL to a filter, then visit that filter to get the attributes
    Option(ECQL.toFilter(ecql_text)
      .accept(new FilterAttributeExtractor, null).asInstanceOf[java.util.HashSet[String]]) match {
      case Some(filterAttributeList) =>
        val schemaAttributeList = targetSchema.getAttributeDescriptors.asScala.map(_.getLocalName)
        filterAttributeList.asScala.forall {
          schemaAttributeList.contains
        }
      case _ => true // null filter that doesn't do anything
    }
  }

  def passThroughFilter(ecql_text: String): Boolean = {
    // convert the ECQL to a filter, then visit that filter to get the attributes
    Option(ECQL.toFilter(ecql_text)
      .accept(new FilterAttributeExtractor, null).asInstanceOf[java.util.HashSet[String]]) match {
      case Some(filterAttributeList) => false // still a non-trivial filter
      case _ => true // null filter that doesn't do anything
    }
  }
}

object IndexIterator extends IteratorHelpers {
//import geomesa.core._
import geomesa.core.index.IndexEntry.IndexEntrySFT  // enriched SimpleFeature to access time attributes

  /**
   * Converts values taken from the Index Value to a SimpleFeature, using the default SimpleFeatureType
   * Note that the ID, taken from the index, is preserved
   * Also note that the requested attributes are not parsed and are instead left as null;
   * the SimpleFeatureFilteringIterator will remove the extraneous attributes later in the Iterator stack
   */
  def encodeIndexValueToSF(featureBuilder: SimpleFeatureBuilder, id: String,
                           geom: Geometry, dtgMillis: Option[Long]): SimpleFeature = {
    val theType = featureBuilder.getFeatureType
    val geomField = theType.getGeometryDescriptor
    // build the feature using the ID extracted from the index
    val nextSimpleFeature = featureBuilder.buildFeature(id)
    // add the geometry field
    nextSimpleFeature.setAttribute(geomField.getLocalName, geom)
    // add the optional time fields, which may not be present in this SimpleFeature
    dtgMillis.map { time => Try {
      nextSimpleFeature.setStartTime(new DateTime(time))
    }
      Try {
        nextSimpleFeature.setEndTime(new DateTime(time))
      }
    }
    nextSimpleFeature
  }

  /**
   * For a given SimpleFeature schema, extract and return a list of the attribute descriptors
   */
  def extractOutputAttributes(targetSchema: String): List[AttributeDescriptor] = {
    val targetSFType = DataUtilities.createType(this.getClass.getCanonicalName, targetSchema)
    targetSFType.getAttributeDescriptors.asScala.toList
  }
}