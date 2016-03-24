/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Collection => jCollection, Map => jMap}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.tables.AttributeTable
import org.locationtech.geomesa.accumulo.transform.TransformCreator
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SerializationType}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Iterator that takes an attribute query that needs only the attribute + (dtg and/or geom)
  * and fulfills the attribute part of the filter from the attr idx.
  */
class AttrKeyPlusValueIterator extends SortedKeyValueIterator[Key, Value] with LazyLogging {
  import AttrKeyPlusValueIterator._

  var source: SortedKeyValueIterator[Key, Value] = null
  var topValue: Value = new Value()

  var origSft:  SimpleFeatureType = null
  var idxSft:   SimpleFeatureType = null
  var transSft: SimpleFeatureType = null

  var indexFilter: Filter = null

  var idxkryo: KryoFeatureSerializer = null
  var idxReuse: KryoBufferSimpleFeature = null

  var nextScalaSf: ScalaSimpleFeature = null

  var origAttrIdx: Integer = null
  var transAttrIdx: Integer = null
  var transFn: SimpleFeature => Array[Byte] = null
  var createAttrFeature: Object => ScalaSimpleFeature = null

  override def init(src: SortedKeyValueIterator[Key, Value],
                    options: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)
    this.source = src.deepCopy(env)
    origSft = SimpleFeatureTypes.createType("o", options.get(ORIG_SFT_OPT))
    origSft.setTableSharing(options.get(ORIG_SFT_TABLE_SHARING).toBoolean)
    idxSft  = SimpleFeatureTypes.createType("i", options.get(IDX_SFT_OPT))
    indexFilter  = Option(options.get(CQL_OPT)).map(FastFilterFactory.toFilter).orNull

    idxkryo  = new KryoFeatureSerializer(idxSft)
    idxReuse = idxkryo.getReusableFeature

    // We should always have a transform here
    val tDefs = options.get(TRANSFORM_DEFINITIONS_OPT)
    transSft  = SimpleFeatureTypes.createType("t", options.get(TRANSFORM_SCHEMA_OPT))
    transFn   = TransformCreator.createTransform(transSft, SerializationType.KRYO, tDefs)

    origAttrIdx = options.get(ATTR_IDX).toInt
    val origAttrDesc = origSft.getDescriptor(origAttrIdx)

    // Setter functions to pull values from the index SF into the projected trans SF
    import scala.collection.JavaConversions._
    val setters: Seq[(KryoBufferSimpleFeature, ScalaSimpleFeature) => Unit] =
      transSft.getAttributeDescriptors
        .map(_.getLocalName)
        .filter(n => idxSft.getAttributeDescriptors.exists(_.getLocalName == n))
        .map { name =>
          val transIdx = transSft.getAttributeDescriptors.indexWhere(_.getLocalName == name)
          val indexIdx = idxSft.getAttributeDescriptors.indexWhere(_.getLocalName == name)
          (ksf: KryoBufferSimpleFeature, sf: ScalaSimpleFeature) => {
            sf.setAttribute(transIdx, ksf.getAttribute(indexIdx))
          }
        }

    // Take the attribute object and create a new SF combined with the Index SF
    transAttrIdx = transSft.getAttributeDescriptors
      .indexWhere(_.getLocalName == origAttrDesc.getLocalName)

    createAttrFeature = (o: Object) => {
      val sf = new ScalaSimpleFeature("", transSft)
      sf.getIdentifier.setID(idxReuse.getID)
      setters.foreach(f => f(idxReuse, sf))
      sf.setAttribute(transAttrIdx, o)
      sf
    }
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
  override def getTopValue: Value = {
    topValue.set(transFn(nextScalaSf))
    topValue
  }


  def findTop(): Unit = {
    var found = false
    while (!found && source.hasTop) {
      idxReuse.setBuffer(source.getTopValue.get())
      if (indexFilter == null || indexFilter.evaluate(idxReuse)) {
        val decoded = AttributeTable.decodeRow(origSft, origAttrIdx, source.getTopKey.getRow().getBytes).get
        nextScalaSf = createAttrFeature(decoded.asInstanceOf[Object])
        found = true
      } else {
        source.next()
      }
    }
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

object AttrKeyPlusValueIterator {

  val ORIG_SFT_OPT              = "orig_sft"                // Original SFT for the feature
  val ORIG_SFT_TABLE_SHARING    = "orig_sft_table_sharing"  // Original SFT table sharing
  val IDX_SFT_OPT               = "idx_sft"                 // SFT of the Attribute Index
  val CQL_OPT                   = "cql"
  val TRANSFORM_SCHEMA_OPT      = "tsft"                    // Transform Schema (target schema)
  val TRANSFORM_DEFINITIONS_OPT = "tdefs"
  val ATTR_IDX                  = "attrIdx"                 // Index of the Attr in original SFT

  def configure(origSft: SimpleFeatureType,
                dataSft: SimpleFeatureType,
                attrIdx: Int,
                indexFilter: Option[Filter], // filter only works on the index value not the index itself
                transform: Option[(String, SimpleFeatureType)],
                priority: Int) = {
    require(transform.isDefined, "No options configured")
    val is = new IteratorSetting(priority, "attr-key-plus-value-iter", classOf[AttrKeyPlusValueIterator])
    is.addOption(ORIG_SFT_OPT, SimpleFeatureTypes.encodeType(origSft))
    is.addOption(ORIG_SFT_TABLE_SHARING, origSft.isTableSharing.toString)
    is.addOption(IDX_SFT_OPT, SimpleFeatureTypes.encodeType(dataSft))
    indexFilter.foreach(f => is.addOption(CQL_OPT, ECQL.toCQL(f)))
    transform.foreach { case (tdef, tsft) =>
      is.addOption(TRANSFORM_DEFINITIONS_OPT, tdef)
      is.addOption(TRANSFORM_SCHEMA_OPT, SimpleFeatureTypes.encodeType(tsft))
    }
    is.addOption(ATTR_IDX, attrIdx.toString)
    is
  }
}
