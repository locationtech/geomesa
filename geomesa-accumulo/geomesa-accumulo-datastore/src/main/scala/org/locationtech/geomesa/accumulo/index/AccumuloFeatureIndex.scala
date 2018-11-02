/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Collections
import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Key, Range, Value}
import org.apache.hadoop.io.Text
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.legacy.attribute._
import org.locationtech.geomesa.accumulo.index.legacy.id.{RecordIndexV1, RecordIndexV2}
import org.locationtech.geomesa.accumulo.index.legacy.z2.{Z2IndexV1, Z2IndexV2, Z2IndexV3}
import org.locationtech.geomesa.accumulo.index.legacy.z3.{Z3IndexV1, Z3IndexV2, Z3IndexV3, Z3IndexV4}
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.accumulo.{AccumuloFeatureIndexType, AccumuloIndexManagerType, AccumuloVersion}
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.{SerializationType, SimpleFeatureDeserializers}
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.locationtech.geomesa.utils.index.{IndexMode, VisibilityLevel}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object AccumuloFeatureIndex extends AccumuloIndexManagerType with LazyLogging {

  val SpatialIndices        = Seq(Z2Index, XZ2Index, Z2IndexV3, Z2IndexV2, Z2IndexV1)
  val SpatioTemporalIndices = Seq(Z3Index, XZ3Index, Z3IndexV4, Z3IndexV3, Z3IndexV2, Z3IndexV1)
  val RecordIndices         = Seq(RecordIndex, RecordIndexV2, RecordIndexV1)
  val AttributeIndices      = Seq(AttributeIndex, AttributeIndexV6, AttributeIndexV5, AttributeIndexV4,
                                    AttributeIndexV3, AttributeIndexV2)

  val DeprecatedSchemaVersionKey = "geomesa.version"

  // note: keep in priority order for running full table scans
  // before changing the order, consider the effect of feature validation in
  // org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
  override val AllIndices: Seq[AccumuloFeatureIndex] =
    SpatioTemporalIndices ++ SpatialIndices ++ RecordIndices ++ AttributeIndices

  override val CurrentIndices: Seq[AccumuloFeatureIndex] =
    Seq(Z3Index, XZ3Index, Z2Index, XZ2Index, RecordIndex, AttributeIndex)

  override def indices(sft: SimpleFeatureType,
                       idx: Option[String] = None,
                       mode: IndexMode = IndexMode.Any): Seq[AccumuloFeatureIndex] =
    super.indices(sft, idx, mode).asInstanceOf[Seq[AccumuloFeatureIndex]]

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

    // note: 10 was the last valid value for CURRENT_SCHEMA_VERSION, which is no longer used except
    // to transition old schemas from the 1.2.5 era
    val version = sft.userData[String](DeprecatedSchemaVersionKey).map(_.toInt).getOrElse(10)
    val indices = if (version > 8) {
      // note: version 9 was never in a release
      Seq(Z3IndexV3, XZ3Index, Z2IndexV2, XZ2Index, RecordIndexV2, AttributeIndexV3)
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

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  protected def hasPrecomputedBins: Boolean

  override def configure(sft: SimpleFeatureType, ds: AccumuloDataStore, partition: Option[String]): String = {
    import scala.collection.JavaConverters._

    val table = super.configure(sft, ds, partition)

    // create table if needed
    AccumuloVersion.ensureTableExists(ds.connector, table, sft.isLogicalTime)

    // create splits
    val splitsToAdd = getSplits(sft, partition).map(new Text(_)).toSet -- ds.tableOps.listSplits(table).asScala.toSet
    if (splitsToAdd.nonEmpty) {
      ds.tableOps.addSplits(table, new java.util.TreeSet(splitsToAdd.asJava))
    }

    // create locality groups
    val existingGroups = ds.tableOps.getLocalityGroups(table)
    val localityGroups = new java.util.HashMap[String, java.util.Set[Text]](existingGroups)

    def addGroup(cf: Text): Unit = {
      val key = cf.toString
      if (localityGroups.containsKey(key)) {
        val update = new java.util.HashSet(localityGroups.get(key))
        update.add(cf)
        localityGroups.put(key, update)
      } else {
        localityGroups.put(key, Collections.singleton(cf))
      }
    }

    AccumuloColumnGroups(sft).foreach { case (k, _) => addGroup(k) }
    if (sft.getVisibilityLevel == VisibilityLevel.Attribute) {
      addGroup(AccumuloColumnGroups.AttributeColumnFamily)
    }
    if (hasPrecomputedBins) {
      addGroup(AccumuloColumnGroups.BinColumnFamily)
    }

    if (localityGroups != existingGroups) {
      ds.tableOps.setLocalityGroups(table, localityGroups)
    }

    // enable block cache
    ds.tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

    table
  }

  override def removeAll(sft: SimpleFeatureType, ds: AccumuloDataStore): Unit = {
    if (TablePartition.partitioned(sft)) {
      // partitioned indices can just drop the partitions
      delete(sft, ds, None)
    } else {
      getTableNames(sft, ds, None).par.foreach { table =>
        if (ds.tableOps.exists(table)) {
          val auths = ds.auths
          val config = GeoMesaBatchWriterConfig().setMaxWriteThreads(ds.config.writeThreads)
          WithClose(ds.connector.createBatchDeleter(table, auths, ds.config.queryThreads, config)) { deleter =>
            val range = if (sft.isTableSharing) {
              val prefix = new Text(sft.getTableSharingBytes)
              new Range(prefix, true, Range.followingPrefix(prefix), false)
            } else {
              new Range()
            }
            deleter.setRanges(Collections.singletonList(range))
            deleter.delete()
          }
        }
      }
    }
  }

  override def delete(sft: SimpleFeatureType, ds: AccumuloDataStore, partition: Option[String]): Unit = {
    val shared = sft.isTableSharing &&
        ds.getTypeNames.filter(_ != sft.getTypeName).map(ds.getSchema).exists(_.isTableSharing)
    if (shared) {
      if (partition.isDefined) {
        throw new IllegalStateException("Found a shared schema with partitioning, which should not be possible")
      }
      removeAll(sft, ds)
    } else {
      getTableNames(sft, ds, partition).par.foreach { table =>
        if (ds.tableOps.exists(table)) {
          // we need to synchronize deleting of tables in mock accumulo as it's not thread safe
          if (ds.connector.isInstanceOf[MockConnector]) {
            ds.connector.synchronized(ds.tableOps.delete(table))
          } else {
            ds.tableOps.delete(table)
          }
        }
      }
    }

    // deletes the metadata
    super.delete(sft, ds, partition)
  }

  // back compatibility check for old metadata keys
  abstract override def getTableNames(sft: SimpleFeatureType,
                                      ds: AccumuloDataStore,
                                      partition: Option[String]): Seq[String] = {
    lazy val oldKey = this match {
      case i if i.name == RecordIndexV1.name  => "tables.record.name"
      case i if i.name == AttributeIndex.name => "tables.idx.attr.name"
      case i => s"tables.${i.name}.name"
    }
    val names = super.getTableNames(sft, ds, partition)
    if (names.isEmpty) {
      ds.metadata.read(sft.getTypeName, oldKey).toSeq
    } else {
      names
    }
  }

  /**
    * Turns accumulo results into simple features
    *
    * @param sft simple feature type
    * @param returnSft return simple feature type (transform, etc)
    * @return
    */
  private [index] def entriesToFeatures(sft: SimpleFeatureType,
                                        returnSft: SimpleFeatureType): Entry[Key, Value] => SimpleFeature = {
    // Perform a projecting decode of the simple feature
    if (serializedWithId) {
      val deserializer = SimpleFeatureDeserializers(returnSft, SerializationType.KRYO)
      kv: Entry[Key, Value] => {
        val sf = deserializer.deserialize(kv.getValue.get)
        AccumuloFeatureIndex.applyVisibility(sf, kv.getKey)
        sf
      }
    } else {
      val getId = getIdFromRow(sft)
      val deserializer = SimpleFeatureDeserializers(returnSft, SerializationType.KRYO, SerializationOptions.withoutId)
      kv: Entry[Key, Value] => {
        val sf = deserializer.deserialize(kv.getValue.get)
        val row = kv.getKey.getRow
        sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row.getBytes, 0, row.getLength, sf))
        AccumuloFeatureIndex.applyVisibility(sf, kv.getKey)
        sf
      }
    }
  }
}
