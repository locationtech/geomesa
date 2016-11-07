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
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.io.Text
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.attribute.AttributeIndexV2
import org.locationtech.geomesa.accumulo.index.id.RecordIndexV1
import org.locationtech.geomesa.accumulo.{AccumuloFeatureIndexType, AccumuloIndexManagerType}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode

import scala.util.Try
import scala.util.control.NonFatal
// noinspection ScalaDeprecation
import org.locationtech.geomesa.accumulo.index.attribute.AttributeIndex
import org.locationtech.geomesa.accumulo.index.z2.{XZ2Index, Z2IndexV1}
import org.locationtech.geomesa.accumulo.index.z3.{XZ3Index, Z3IndexV1, Z3IndexV2}
// noinspection ScalaDeprecation
import org.locationtech.geomesa.accumulo.index.id.RecordIndex
import org.locationtech.geomesa.accumulo.index.z2.Z2Index
import org.locationtech.geomesa.accumulo.index.z3.Z3Index
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.{SerializationType, SimpleFeatureDeserializers}
import org.locationtech.geomesa.security.SecurityUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

// noinspection ScalaDeprecation
object AccumuloFeatureIndex extends AccumuloIndexManagerType with LazyLogging {

  private val SpatialIndices        = Seq(Z2Index, XZ2Index, Z2IndexV1)
  private val SpatioTemporalIndices = Seq(Z3Index, XZ3Index, Z3IndexV2, Z3IndexV1)
  private val AttributeIndices      = Seq(AttributeIndex, AttributeIndexV2)
  private val RecordIndices         = Seq(RecordIndex, RecordIndexV1)

  // note: keep in priority order for running full table scans
  override val AllIndices: Seq[AccumuloWritableIndex] =
    SpatioTemporalIndices ++ SpatialIndices ++ RecordIndices ++ AttributeIndices

  override val CurrentIndices: Seq[AccumuloWritableIndex] =
    Seq(Z3Index, XZ3Index, Z2Index, XZ2Index, RecordIndex, AttributeIndex)

  override def indices(sft: SimpleFeatureType, mode: IndexMode): Seq[AccumuloWritableIndex] =
    super.indices(sft, mode).asInstanceOf[Seq[AccumuloWritableIndex]]

  override def index(identifier: String): AccumuloWritableIndex =
    super.index(identifier).asInstanceOf[AccumuloWritableIndex]

  override def lookup: Map[(String, Int), AccumuloWritableIndex] =
    super.lookup.asInstanceOf[Map[(String, Int), AccumuloWritableIndex]]

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
  def replaces(index: AccumuloFeatureIndexType,
               existing: Seq[AccumuloFeatureIndexType]): Option[AccumuloFeatureIndexType] = {
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

trait AccumuloWritableIndex extends AccumuloFeatureIndexType {

  override def delete(sft: SimpleFeatureType, ds: AccumuloDataStore, shared: Boolean): Unit = {
    import org.apache.accumulo.core.data.{Range => aRange}
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    val table = getTableName(sft.getTypeName, ds)
    if (ds.tableOps.exists(table)) {
      if (shared) {
        val auths = ds.config.authProvider.getAuthorizations
        val config = GeoMesaBatchWriterConfig().setMaxWriteThreads(ds.config.writeThreads)
        val prefix = new Text(sft.getTableSharingPrefix)
        val deleter = ds.connector.createBatchDeleter(table, auths, ds.config.queryThreads, config)
        try {
          deleter.setRanges(Seq(new aRange(prefix, true, aRange.followingPrefix(prefix), false)))
          deleter.delete()
        } finally {
          deleter.close()
        }
      } else {
        ds.tableOps.delete(table)
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

  // back compatibility check for old metadata keys
  abstract override def getTableName(typeName: String, ds: AccumuloDataStore): String = {
    lazy val oldKey = this match {
      case i if i.name == RecordIndex.name    => "tables.record.name"
      case i if i.name == AttributeIndex.name => "tables.idx.attr.name"
      case i => s"tables.${i.name}.name"
    }
    Try(super.getTableName(typeName, ds)).recoverWith {
      case NonFatal(e) => Try(ds.metadata.read(typeName, oldKey).getOrElse(throw e))
    }.get
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