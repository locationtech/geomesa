/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Collection => jCollection, Map => jMap}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
 * Iterator that operates on kryo encoded values. It will:
 *   a) evaluate the feature against an optional filter
 *   b) apply an optional transform
 *
 * Uses lazy evaluation of attributes and binary transforms when possible.
 */
class KryoLazyFilterTransformIterator extends SortedKeyValueIterator[Key, Value] with Logging {

  import KryoLazyFilterTransformIterator._

  var source: SortedKeyValueIterator[Key, Value] = null

  var sft: SimpleFeatureType = null
  var filter: Filter = null
  var topValue: Value = new Value()

  var kryo: KryoFeatureSerializer = null
  var reusablesf: KryoBufferSimpleFeature = null
  var hasTransform: Boolean = false

  override def init(src: SortedKeyValueIterator[Key, Value],
                    options: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)
    this.source = src.deepCopy(env)
    sft = SimpleFeatureTypes.createType("test", options.get(SFT_OPT))
    filter = Option(options.get(CQL_OPT)).map(FastFilterFactory.toFilter).orNull

    kryo = new KryoFeatureSerializer(sft)
    reusablesf = kryo.getReusableFeature

    val transform = Option(options.get(TRANSFORM_DEFINITIONS_OPT))
    val transformSchema = Option(options.get(TRANSFORM_SCHEMA_OPT))
    for { t <- transform; ts <- transformSchema } {
      reusablesf.setTransforms(t, SimpleFeatureTypes.createType("", ts))
    }
    hasTransform = transform.isDefined
  }

  override def seek(range: Range, columnFamilies: jCollection[ByteSequence], inclusive: Boolean): Unit = {
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def next(): Unit = {
    source.next()
    findTop()
  }

  override def hasTop: Boolean = source.hasTop
  override def getTopKey: Key = source.getTopKey
  override def getTopValue: Value =
    if (hasTransform) {
      topValue.set(reusablesf.transform())
      topValue
    } else {
      source.getTopValue
    }

  def findTop(): Unit = {
    var found = false
    while (!found && source.hasTop) {
      reusablesf.setBuffer(source.getTopValue.get())
      if (filter == null || filter.evaluate(reusablesf)) {
        found = true
      } else {
        source.next()
      }
    }
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

object KryoLazyFilterTransformIterator {

  val SFT_OPT                   = "sft"
  val CQL_OPT                   = "cql"
  val TRANSFORM_SCHEMA_OPT      = "tsft"
  val TRANSFORM_DEFINITIONS_OPT = "tdefs"

  def configure(sft: SimpleFeatureType,
                filter: Option[Filter],
                transform: Option[(String, SimpleFeatureType)],
                priority: Int) = {
    require(filter.isDefined || transform.isDefined, "No options configured")
    val is = new IteratorSetting(priority, "filter-transform-iter", classOf[KryoLazyFilterTransformIterator])
    is.addOption(SFT_OPT, SimpleFeatureTypes.encodeType(sft))
    filter.foreach(f => is.addOption(CQL_OPT, ECQL.toCQL(f)))
    transform.foreach { case (tdef, tsft) =>
      is.addOption(TRANSFORM_DEFINITIONS_OPT, tdef)
      is.addOption(TRANSFORM_SCHEMA_OPT, SimpleFeatureTypes.encodeType(tsft))
    }
    is
  }
}