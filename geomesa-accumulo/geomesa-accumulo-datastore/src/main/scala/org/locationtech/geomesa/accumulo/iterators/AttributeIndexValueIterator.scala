/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.index.index.attribute.AttributeIndex.AttributeRowDecoder
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Runs at the bottom of the iterator stack - takes the index simple feature type and adds the value
  * from the row key
  */
class AttributeIndexValueIterator extends SortedKeyValueIterator[Key, Value] with LazyLogging {

  import AttributeIndexValueIterator._

  private var source: SortedKeyValueIterator[Key, Value] = _
  private val topValue: Value = new Value

  private var decodeRowValue: (Array[Byte], Int, Int) => Try[Any] = _
  private var serializer: KryoFeatureSerializer = _
  private var attribute: Int = -1

  private var original: KryoBufferSimpleFeature = _
  private var withAttribute: ScalaSimpleFeature = _
  private var mappings: Array[(Int, Int)] = _
  private var setId: (Text) => Unit = _

  private var cql: Option[Filter] = _

  override def init(src: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = src

    val spec = options.get(SFT_OPT)
    val sft = IteratorCache.sft(spec)

    val transformSpec = options.get(TRANSFORM_OPT)
    val transformSft = IteratorCache.sft(transformSpec)

    attribute = options.get(ATTRIBUTE_OPT).toInt
    mappings = Array.tabulate(transformSft.getAttributeCount - 1) { i =>
      if (i < attribute) {
        (i, sft.indexOf(transformSft.getDescriptor(i).getLocalName))
      } else {
        (i + 1, sft.indexOf(transformSft.getDescriptor(i + 1).getLocalName))
      }
    }

    cql = Option(options.get(CQL_OPT)).map(IteratorCache.filter(sft, spec, _))

    val index = try {
      AccumuloFeatureIndex.index(options.get(INDEX_OPT)).asInstanceOf[AccumuloFeatureIndex with AttributeRowDecoder]
    } catch {
      case NonFatal(e) => throw new RuntimeException(s"Index option not configured correctly: ${options.get(INDEX_OPT)}")
    }
    // noinspection ScalaDeprecation
    val kryoOptions = if (index.serializedWithId) { SerializationOptions.none } else { SerializationOptions.withoutId }

    original = IteratorCache.serializer(spec, kryoOptions).getReusableFeature
    withAttribute = new ScalaSimpleFeature(transformSft, "")

    serializer = IteratorCache.serializer(transformSpec, kryoOptions)
    decodeRowValue = index.decodeRowValue(transformSft, attribute)
    // noinspection ScalaDeprecation
    setId = if (index.serializedWithId || cql.isEmpty) { (_) => {} } else {
      val getFromRow = index.getIdFromRow(sft)
      (row) => original.setId(getFromRow(row.getBytes, 0, row.getLength, original))
    }
  }

  override def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def next(): Unit = {
    source.next()
    findTop()
  }
  override def hasTop: Boolean = source.hasTop
  override def getTopKey: Key = source.getTopKey
  override def getTopValue: Value = topValue

  def findTop(): Unit = {
    var found = false
    while (!found && source.hasTop) {
      val row = source.getTopKey.getRow
      decodeRowValue(row.getBytes, 0, row.getLength) match {
        case Failure(_)     => source.next()
        case Success(value) =>
          original.setBuffer(source.getTopValue.get())
          setId(row)
          if (cql.forall(_.evaluate(original))) {
            mappings.foreach { case (to, from) => withAttribute.setAttributeNoConvert(to, original.getAttribute(from)) }
            withAttribute.setAttributeNoConvert(attribute, value.asInstanceOf[AnyRef])
            withAttribute.setId(original.getID)
            topValue.set(serializer.serialize(withAttribute))
            found = true
          } else {
            source.next()
          }
      }
    }
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] =
    throw new NotImplementedError
}

object AttributeIndexValueIterator {

  val INDEX_OPT     = "index"
  val SFT_OPT       = "sft"
  val TRANSFORM_OPT = "tsft"
  val ATTRIBUTE_OPT = "attr"
  val CQL_OPT       = "cql"

  val DefaultPriority = 23 // needs to be lower than aggregating iterators at 25

  def configure(index: AccumuloFeatureIndexType,
                indexSft: SimpleFeatureType,
                transform: SimpleFeatureType,
                attribute: String,
                ecql: Option[Filter],
                priority: Int = DefaultPriority): IteratorSetting = {
    val is = new IteratorSetting(priority, "attr-index-iter", classOf[AttributeIndexValueIterator])
    is.addOption(INDEX_OPT, index.identifier)
    is.addOption(SFT_OPT, SimpleFeatureTypes.encodeType(indexSft, includeUserData = true))
    is.addOption(TRANSFORM_OPT, SimpleFeatureTypes.encodeType(transform, includeUserData = true))
    is.addOption(ATTRIBUTE_OPT, transform.indexOf(attribute).toString)
    ecql.foreach(e => is.addOption(CQL_OPT, ECQL.toCQL(e)))
    is
  }
}