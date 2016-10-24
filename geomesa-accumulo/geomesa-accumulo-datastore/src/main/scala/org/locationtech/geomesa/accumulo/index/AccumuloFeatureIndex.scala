/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index.attribute.AttributeIndexV2
import org.locationtech.geomesa.accumulo.index.id.RecordIndexV1
// noinspection ScalaDeprecation
import org.locationtech.geomesa.accumulo.index.attribute.AttributeIndex
import org.locationtech.geomesa.accumulo.index.z2.{XZ2Index, Z2IndexV1}
import org.locationtech.geomesa.accumulo.index.z3.{XZ3Index, Z3IndexV1, Z3IndexV2}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
// noinspection ScalaDeprecation
import org.locationtech.geomesa.accumulo.index.id.RecordIndex
import org.locationtech.geomesa.accumulo.index.z2.Z2Index
import org.locationtech.geomesa.accumulo.index.z3.Z3Index
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.{SerializationType, SimpleFeatureDeserializers}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.security.SecurityUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

// noinspection ScalaDeprecation
object AccumuloFeatureIndex extends LazyLogging {

  type AccumuloFeatureIndex = GeoMesaFeatureIndex[AccumuloDataStore, WritableFeature, Seq[Mutation], QueryPlan]
  type AccumuloFilterPlan = FilterPlan[AccumuloDataStore, WritableFeature, Seq[Mutation], QueryPlan]
  type AccumuloFilterStrategy = FilterStrategy[AccumuloDataStore, WritableFeature, Seq[Mutation], QueryPlan]

  private val SpatialIndices        = Seq(Z2Index, XZ2Index, Z2IndexV1)
  private val SpatioTemporalIndices = Seq(Z3Index, XZ3Index, Z3IndexV2, Z3IndexV1)
  private val AttributeIndices      = Seq(AttributeIndex, AttributeIndexV2)
  private val RecordIndices         = Seq(RecordIndex, RecordIndexV1)

  // note: keep in priority order for running full table scans
  val AllIndices: Seq[AccumuloWritableIndex] =
    SpatioTemporalIndices ++ SpatialIndices ++ RecordIndices ++ AttributeIndices

  val CurrentIndices: Seq[AccumuloWritableIndex] =
    Seq(Z3Index, XZ3Index, Z2Index, XZ2Index, RecordIndex, AttributeIndex)

  val IndexLookup = AllIndices.map(i => (i.name, i.version) -> i).toMap

  def indices(sft: SimpleFeatureType, mode: IndexMode): Seq[AccumuloWritableIndex] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val withMode = sft.getIndices.filter { case (_, _, m) => m.supports(mode) }
    // filter the list of all indices so that we maintain priority order
    AllIndices.filter(i => withMode.exists { case (n, v, _) => i.name == n && i.version == v})
  }

  def index(identifier: String): AccumuloWritableIndex = {
    val Array(n, v) = identifier.split(":")
    IndexLookup(n, v.toInt)
  }

  object Schemes {
    val Z3TableScheme: List[String] = List(AttributeIndex, RecordIndex, Z3Index, XZ3Index).map(_.name)
    val Z2TableScheme: List[String] = List(AttributeIndex, RecordIndex, Z2Index, XZ2Index).map(_.name)
  }

  /**
    * Look up the existing index that could be replaced by the new index, if any
    *
    * @param index new index
    * @param existing list of existing indices
    * @return
    */
  def replaces(index: AccumuloFeatureIndex, existing: Seq[AccumuloFeatureIndex]): Option[AccumuloFeatureIndex] = {
    if (SpatialIndices.contains(index)) {
      existing.find(SpatialIndices.contains)
    } else if (SpatioTemporalIndices.contains(index)) {
      existing.find(SpatioTemporalIndices.contains)
    } else if (AttributeIndices.contains(index)) {
      existing.find(AttributeIndices.contains)
    } else {
      None
    }
  }

  /**
    * Maps a simple feature type to a set of default indices based on
    * attributes and when it was created (schema version)
    *
    * @param sft simple feature type
    * @return
    */
  def getDefaultIndices(sft: SimpleFeatureType): Seq[AccumuloWritableIndex] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    lazy val docs =
      "http://www.geomesa.org/documentation/user/jobs.html#updating-existing-data-to-the-latest-index-format"
    val version = sft.getSchemaVersion
    val indices = if (version > 8) {
      CurrentIndices // note: version 9 was never in a release
    } else if (version == 8) {
      Seq(Z3IndexV2, Z2IndexV1, RecordIndexV1, AttributeIndexV2)
    } else if (version > 5) {
      logger.warn("The GeoHash index is no longer supported. Some queries make take longer than normal. To " +
          s"update your data to a newer format, see $docs")
      version match {
        case 7 => Seq(Z3IndexV2, RecordIndexV1, AttributeIndexV2)
        case 6 => Seq(Z3IndexV1, RecordIndexV1, AttributeIndexV2)
      }
    } else {
      throw new NotImplementedError("This schema format is no longer supported. Please use " +
          s"GeoMesa 1.2.6+ to update you data to a newer format. For more information, see $docs")
    }
    indices.filter(_.supports(sft))
  }
}

trait AccumuloWritableIndex extends AccumuloFeatureIndex {

  def tableNameKey: String = s"table.$name.v$version"

  def tableSuffix: String = if (version == 1) name else s"${name}_v$version"

  override def removeAll(sft: SimpleFeatureType, ops: AccumuloDataStore): Unit = {
    import org.apache.accumulo.core.data.{Range => aRange}
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    val table = ops.getTableName(sft.getTypeName, this)
    if (ops.tableOps.exists(table)) {
      val auths = ops.authProvider.getAuthorizations
      val config = GeoMesaBatchWriterConfig().setMaxWriteThreads(ops.config.writeThreads)
      val prefix = new Text(sft.getTableSharingPrefix)
      val deleter = ops.connector.createBatchDeleter(table, auths, ops.config.queryThreads, config)
      try {
        deleter.setRanges(Seq(new aRange(prefix, true, aRange.followingPrefix(prefix), false)))
        deleter.delete()
      } finally {
        deleter.close()
      }
    }
  }

  /**
    * Retrieve an ID from a row. All indices are assumed to encode the feature ID into the row key
    *
    * @param sft simple feature type
    * @return a function to retrieve an ID from a row
    */
  def getIdFromRow(sft: SimpleFeatureType): (Text) => String

  /**
    * Indicates whether the ID for each feature is serialized with the feature or in the row
    *
    * @return
    */
  def serializedWithId: Boolean

  /**
    * Turns accumulo results into simple features
    *
    * @param sft simple feature type
    * @param returnSft return simple feature type (transform, etc)
    * @return
    */
  def entriesToFeatures(sft: SimpleFeatureType, returnSft: SimpleFeatureType): (Entry[Key, Value]) => SimpleFeature = {
    // Perform a projecting decode of the simple feature
    if (serializedWithId) {
      val deserializer = SimpleFeatureDeserializers(returnSft, SerializationType.KRYO)
      (kv: Entry[Key, Value]) => {
        val sf = deserializer.deserialize(kv.getValue.get)
        AccumuloWritableIndex.applyVisibility(sf, kv.getKey)
        sf
      }
    } else {
      val getId = getIdFromRow(sft)
      val deserializer = SimpleFeatureDeserializers(returnSft, SerializationType.KRYO, SerializationOptions.withoutId)
      (kv: Entry[Key, Value]) => {
        val sf = deserializer.deserialize(kv.getValue.get)
        sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(kv.getKey.getRow))
        AccumuloWritableIndex.applyVisibility(sf, kv.getKey)
        sf
      }
    }
  }
}

object AccumuloWritableIndex {

  val DefaultNumSplits = 4 // can't be more than Byte.MaxValue (127)
  val DefaultSplitArrays = (0 until DefaultNumSplits).map(_.toByte).toArray.map(Array(_)).toSeq

  val NullByte = Array(0.toByte)
  val EmptyBytes = Array.empty[Byte]

  val FullColumnFamily      = new Text("F")
  val IndexColumnFamily     = new Text("I")
  val BinColumnFamily       = new Text("B")
  val AttributeColumnFamily = new Text("A")

  val EmptyColumnQualifier  = new Text()

  val EmptyValue = new Value(EmptyBytes)

  def applyVisibility(sf: SimpleFeature, key: Key): Unit = {
    val visibility = key.getColumnVisibility
    if (visibility.getLength > 0) {
      SecurityUtils.setFeatureVisibility(sf, visibility.toString)
    }
  }
}