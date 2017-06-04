/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Map.Entry

import com.google.common.collect.{ImmutableSet, ImmutableSortedSet}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Key, Range, Value}
import org.apache.hadoop.io.Text
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.legacy.attribute.{AttributeIndexV2, AttributeIndexV3, AttributeIndexV4}
import org.locationtech.geomesa.accumulo.index.legacy.id.RecordIndexV1
import org.locationtech.geomesa.accumulo.index.legacy.z2.{Z2IndexV1, Z2IndexV2}
import org.locationtech.geomesa.accumulo.index.legacy.z3.{Z3IndexV1, Z3IndexV2, Z3IndexV3}
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.accumulo.{AccumuloFeatureIndexType, AccumuloIndexManagerType, AccumuloVersion}
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.{SerializationType, SimpleFeatureDeserializers}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.Try
import scala.util.control.NonFatal

object AccumuloFeatureIndex extends AccumuloIndexManagerType with LazyLogging {

  val FullColumnFamily      = new Text("F")
  val IndexColumnFamily     = new Text("I")
  val BinColumnFamily       = new Text("B")
  val AttributeColumnFamily = new Text("A")

  val EmptyColumnQualifier  = new Text()

  private val SpatialIndices        = Seq(Z2Index, XZ2Index, Z2IndexV2, Z2IndexV1)
  private val SpatioTemporalIndices = Seq(Z3Index, XZ3Index, Z3IndexV3, Z3IndexV2, Z3IndexV1)
  private val AttributeIndices      = Seq(AttributeIndex, AttributeIndexV4, AttributeIndexV3, AttributeIndexV2)
  private val RecordIndices         = Seq(RecordIndex, RecordIndexV1)

  // note: keep in priority order for running full table scans
  // before changing the order, consider the effect of feature validation in
  // org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
  override val AllIndices: Seq[AccumuloFeatureIndex] =
    SpatioTemporalIndices ++ SpatialIndices ++ RecordIndices ++ AttributeIndices

  override val CurrentIndices: Seq[AccumuloFeatureIndex] =
    Seq(Z3Index, XZ3Index, Z2Index, XZ2Index, RecordIndex, AttributeIndex)

  override def indices(sft: SimpleFeatureType, mode: IndexMode): Seq[AccumuloFeatureIndex] =
    super.indices(sft, mode).asInstanceOf[Seq[AccumuloFeatureIndex]]

  override def index(identifier: String): AccumuloFeatureIndex =
    super.index(identifier).asInstanceOf[AccumuloFeatureIndex]

  override def lookup: Map[(String, Int), AccumuloFeatureIndex] =
    super.lookup.asInstanceOf[Map[(String, Int), AccumuloFeatureIndex]]

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
    * attributes and when it was created (schema version).
    *
    * Note that schema version has been deprecated and this method should not be called
    * except when transitioning from schema version to per-index versions.
    *
    * @param sft simple feature type
    * @return
    */
  def getDefaultIndices(sft: SimpleFeatureType): Seq[AccumuloFeatureIndex] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    lazy val docs =
      "http://www.geomesa.org/documentation/user/jobs.html#updating-existing-data-to-the-latest-index-format"
    val version = sft.getSchemaVersion
    val indices = if (version > 8) {
      // note: version 9 was never in a release
      Seq(Z3IndexV3, XZ3Index, Z2IndexV2, XZ2Index, RecordIndex, AttributeIndexV3)
    } else if (version == 8) {
      Seq(Z3IndexV2, Z2IndexV1, RecordIndexV1, AttributeIndexV2)
    } else if (version > 5) {
      logger.warn("The GeoHash index is no longer supported. Some queries may take longer than normal. To " +
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

  def applyVisibility(sf: SimpleFeature, key: Key): Unit = {
    val visibility = key.getColumnVisibility
    if (visibility.getLength > 0) {
      SecurityUtils.setFeatureVisibility(sf, visibility.toString)
    }
  }
}

trait AccumuloFeatureIndex extends AccumuloFeatureIndexType {

  import AccumuloFeatureIndex.{AttributeColumnFamily, BinColumnFamily, FullColumnFamily}
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  /**
    * Indicates whether the ID for each feature is serialized with the feature or in the row
    *
    * @return
    */
  def serializedWithId: Boolean

  protected def hasPrecomputedBins: Boolean

  override def configure(sft: SimpleFeatureType, ds: AccumuloDataStore): Unit = {
    import scala.collection.JavaConversions._

    super.configure(sft, ds)

    val table = getTableName(sft.getTypeName, ds)

    // create table if needed
    AccumuloVersion.ensureTableExists(ds.connector, table)

    // create splits
    val splitsToAdd = getSplits(sft).map(new Text(_)).toSet -- ds.tableOps.listSplits(table).toSet
    if (splitsToAdd.nonEmpty) {
      // noinspection RedundantCollectionConversion
      ds.tableOps.addSplits(table, ImmutableSortedSet.copyOf(splitsToAdd.toIterable))
    }

    // create locality groups
    val cfs = if (hasPrecomputedBins) {
      Seq(FullColumnFamily, BinColumnFamily, AttributeColumnFamily)
    } else {
      Seq(FullColumnFamily, AttributeColumnFamily)
    }
    val localityGroups = cfs.map(cf => (cf.toString, ImmutableSet.of(cf))).toMap
    ds.tableOps.setLocalityGroups(table, localityGroups)

    // enable block cache
    ds.tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")
  }

  override def delete(sft: SimpleFeatureType, ds: AccumuloDataStore, shared: Boolean): Unit = {
    import scala.collection.JavaConversions._

    val table = getTableName(sft.getTypeName, ds)
    if (ds.tableOps.exists(table)) {
      if (shared) {
        val auths = ds.config.authProvider.getAuthorizations
        val config = GeoMesaBatchWriterConfig().setMaxWriteThreads(ds.config.writeThreads)
        val prefix = new Text(sft.getTableSharingPrefix)
        val deleter = ds.connector.createBatchDeleter(table, auths, ds.config.queryThreads, config)
        try {
          deleter.setRanges(Seq(new Range(prefix, true, Range.followingPrefix(prefix), false)))
          deleter.delete()
        } finally {
          deleter.close()
        }
      } else {
        // we need to synchronize deleting of tables in mock accumulo as it's not thread safe
        if (ds.connector.isInstanceOf[MockConnector]) {
          ds.connector.synchronized(ds.tableOps.delete(table))
        } else {
          ds.tableOps.delete(table)
        }
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

  /**
    * Turns accumulo results into simple features
    *
    * @param sft simple feature type
    * @param returnSft return simple feature type (transform, etc)
    * @return
    */
  private [index] def entriesToFeatures(sft: SimpleFeatureType,
                                        returnSft: SimpleFeatureType): (Entry[Key, Value]) => SimpleFeature = {
    // Perform a projecting decode of the simple feature
    if (serializedWithId) {
      val deserializer = SimpleFeatureDeserializers(returnSft, SerializationType.KRYO)
      (kv: Entry[Key, Value]) => {
        val sf = deserializer.deserialize(kv.getValue.get)
        AccumuloFeatureIndex.applyVisibility(sf, kv.getKey)
        sf
      }
    } else {
      val getId = getIdFromRow(sft)
      val deserializer = SimpleFeatureDeserializers(returnSft, SerializationType.KRYO, SerializationOptions.withoutId)
      (kv: Entry[Key, Value]) => {
        val sf = deserializer.deserialize(kv.getValue.get)
        val row = kv.getKey.getRow
        sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row.getBytes, 0, row.getLength))
        AccumuloFeatureIndex.applyVisibility(sf, kv.getKey)
        sf
      }
    }
  }
}
