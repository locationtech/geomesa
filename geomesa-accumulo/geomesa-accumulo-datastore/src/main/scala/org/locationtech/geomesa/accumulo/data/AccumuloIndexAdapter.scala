/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.Map.Entry

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 120815d0b0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 234aacdc12 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 63a045a753 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 19646771be (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f0 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 5e469d5cd9 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
=======
>>>>>>> 51a90e7f04 (GEOMESA-3092 Support Lambda NiFi processor (#2777))
>>>>>>> 9b0960d94d (GEOMESA-3092 Support Lambda NiFi processor (#2777))
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Key, Range, Value}
import org.apache.accumulo.core.file.keyfunctor.RowFunctor
import org.apache.hadoop.io.Text
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.accumulo.data.AccumuloIndexAdapter.{AccumuloResultsToFeatures, ZIterPriority}
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.{BatchScanPlan, EmptyPlan}
import org.locationtech.geomesa.accumulo.data.writer.tx.AccumuloAtomicIndexWriter
import org.locationtech.geomesa.accumulo.data.writer.{AccumuloIndexWriter, ColumnFamilyMapper}
import org.locationtech.geomesa.accumulo.index.{AttributeJoinIndex, JoinIndex}
import org.locationtech.geomesa.accumulo.iterators.ArrowIterator.AccumuloArrowResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator.AccumuloBinResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators.DensityIterator.AccumuloDensityResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators.StatsIterator.AccumuloStatsResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.accumulo.util.TableUtils
import org.locationtech.geomesa.index.api.IndexAdapter.{IndexWriter, RequiredVisibilityWriter}
import org.locationtech.geomesa.index.api.QueryPlan.IndexResultsToFeatures
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.s2.{S2Index, S2IndexValues}
import org.locationtech.geomesa.index.index.s3.{S3Index, S3IndexValues}
import org.locationtech.geomesa.index.index.z2.{Z2Index, Z2IndexValues}
import org.locationtech.geomesa.index.index.z3.{Z3Index, Z3IndexValues}
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.planning.LocalQueryRunner.{ArrowDictionaryHook, LocalTransformReducer}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.locationtech.geomesa.utils.io.WithClose

import java.util.Collections
import java.util.Map.Entry

/**
  * Index adapter for accumulo back-end
  *
  * @param ds data store
  */
class AccumuloIndexAdapter(ds: AccumuloDataStore) extends IndexAdapter[AccumuloDataStore] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  private val tableOps = ds.connector.tableOperations()

  // noinspection ScalaDeprecation
  override def createTable(
      index: GeoMesaFeatureIndex[_, _],
      partition: Option[String],
      splits: => Seq[Array[Byte]]): Unit = {
    val table = index.configureTableName(partition) // writes table name to metadata
    // create table if it doesn't exist
    val created = TableUtils.createTableIfNeeded(ds.connector, table, index.sft.isLogicalTime)

    def addSplitsAndGroups(): Unit = {
      // create splits
      val splitsToAdd = splits.map(new Text(_)).toSet -- tableOps.listSplits(table).asScala.toSet
      if (splitsToAdd.nonEmpty) {
        tableOps.addSplits(table, new java.util.TreeSet(splitsToAdd.asJava))
      }

      // create locality groups
      val existingGroups = tableOps.getLocalityGroups(table)
      val localityGroups = new java.util.HashMap[String, java.util.Set[Text]](existingGroups)

      def addGroup(cf: Text): Unit = {
        val key = cf.toString
        if (localityGroups.containsKey(key)) {
          val update = new java.util.HashSet[Text](localityGroups.get(key))
          update.add(cf)
          localityGroups.put(key, update)
        } else {
          localityGroups.put(key, Collections.singleton(cf))
        }
      }

      groups.apply(index.sft).foreach { case (k, _) => addGroup(new Text(k)) }

      if (localityGroups != existingGroups) {
        tableOps.setLocalityGroups(table, localityGroups)
      }
    }

    if (created) {
      addSplitsAndGroups()

      // enable block cache
      tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

      if (index.name == IdIndex.name) {
        // enable the row functor as the feature ID is stored in the Row ID
        tableOps.setProperty(table, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey, classOf[RowFunctor].getCanonicalName)
        tableOps.setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey, "true")
      }

      if (index.sft.isVisibilityRequired) {
        VisibilityIterator.set(tableOps, table)
      }
    } else if (index.keySpace.sharing.nonEmpty) {
      // even if the table existed, we still need to check the splits and locality groups if it's shared
      addSplitsAndGroups()
    }
  }

  override def renameTable(from: String, to: String): Unit = {
    if (tableOps.exists(from)) {
      tableOps.rename(from, to)
    }
  }

  override def deleteTables(tables: Seq[String]): Unit = {
    def deleteOne(table: String): Unit = {
      if (tableOps.exists(table)) {
        tableOps.delete(table)
      }
    }
    tables.toList.map(table => CachedThreadPool.submit(() => deleteOne(table))).foreach(_.get)
  }

  override def clearTables(tables: Seq[String], prefix: Option[Array[Byte]]): Unit = {
    val auths = ds.auths // get the auths once up front
    def clearOne(table: String): Unit = {
      if (tableOps.exists(table)) {
        WithClose(ds.connector.createBatchDeleter(table, auths, ds.config.queries.threads)) { deleter =>
          val range = prefix.map(p => Range.prefix(new Text(p))).getOrElse(new Range())
          deleter.setRanges(Collections.singletonList(range))
          deleter.delete()
        }
      }
    }
    tables.toList.map(table => CachedThreadPool.submit(() => clearOne(table))).foreach(_.get)
  }

  override def createQueryPlan(strategy: QueryStrategy): AccumuloQueryPlan = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val QueryStrategy(filter, byteRanges, _, _, ecql, hints, _) = strategy
    val index = filter.index
    // index api defines empty start/end for open-ended range - in accumulo, it's indicated with null
    // index api defines start row inclusive, end row exclusive
    val ranges = byteRanges.map {
      case BoundedByteRange(start, end) =>
          val startKey = if (start.length == 0) { null } else { new Key(new Text(start)) }
          val endKey = if (end.length == 0) { null } else { new Key(new Text(end)) }
          new Range(startKey, true, endKey, false)

      case SingleRowByteRange(row) =>
        new Range(new Text(row))
    }
    val numThreads = if (index.name == IdIndex.name) { ds.config.queries.recordThreads } else { ds.config.queries.threads }
    val tables = index.getTablesForQuery(filter.filter)
    val (colFamily, schema) = {
      val (cf, s) = groups.group(index.sft, hints.getTransformDefinition, ecql)
      (Some(new Text(ColumnFamilyMapper(index)(cf))), s)
    }
    // used when remote processing is disabled
    lazy val returnSchema = hints.getTransformSchema.getOrElse(schema)
    lazy val fti = FilterTransformIterator.configure(schema, index, ecql, hints).toSeq
    lazy val resultsToFeatures = AccumuloResultsToFeatures(index, returnSchema)
    lazy val localReducer = {
      val arrowHook = Some(ArrowDictionaryHook(ds.stats, filter.filter))
      Some(new LocalTransformReducer(returnSchema, None, None, None, hints, arrowHook))
    }

    index match {
      case i: AttributeJoinIndex =>
        AccumuloJoinIndexAdapter.createQueryPlan(ds, i, filter, tables, ranges, colFamily, schema, ecql, hints, numThreads)

      case _ =>
        val (iter, eToF, reduce) = if (strategy.hints.isBinQuery) {
          if (ds.config.remote.bin) {
            val iter = BinAggregatingIterator.configure(schema, index, ecql, hints)
            (Seq(iter), new AccumuloBinResultsToFeatures(), None)
          } else {
            if (hints.isSkipReduce) {
              // override the return sft to reflect what we're actually returning,
              // since the bin sft is only created in the local reduce step
              hints.hints.put(QueryHints.Internal.RETURN_SFT, returnSchema)
            }
            (fti, resultsToFeatures, localReducer)
          }
        } else if (strategy.hints.isArrowQuery) {
          if (ds.config.remote.arrow) {
            val (iter, reduce) = ArrowIterator.configure(schema, index, ds.stats, filter.filter, ecql, hints)
            (Seq(iter), new AccumuloArrowResultsToFeatures(), Some(reduce))
          } else {
            if (hints.isSkipReduce) {
              // override the return sft to reflect what we're actually returning,
              // since the arrow sft is only created in the local reduce step
              hints.hints.put(QueryHints.Internal.RETURN_SFT, returnSchema)
            }
            (fti, resultsToFeatures, localReducer)
          }
        } else if (strategy.hints.isDensityQuery) {
          if (ds.config.remote.density) {
            val iter = DensityIterator.configure(schema, index, ecql, hints)
            (Seq(iter), new AccumuloDensityResultsToFeatures(), None)
          } else {
            if (hints.isSkipReduce) {
              // override the return sft to reflect what we're actually returning,
              // since the density sft is only created in the local reduce step
              hints.hints.put(QueryHints.Internal.RETURN_SFT, returnSchema)
            }
            (fti, resultsToFeatures, localReducer)
          }
        } else if (strategy.hints.isStatsQuery) {
          if (ds.config.remote.stats) {
            val iter = StatsIterator.configure(schema, index, ecql, hints)
            val reduce = Some(StatsScan.StatsReducer(schema, hints))
            (Seq(iter), new AccumuloStatsResultsToFeatures(), reduce)
          } else {
            if (hints.isSkipReduce) {
              // override the return sft to reflect what we're actually returning,
              // since the stats sft is only created in the local reduce step
              hints.hints.put(QueryHints.Internal.RETURN_SFT, returnSchema)
            }
            (fti, resultsToFeatures, localReducer)
          }
        } else {
          (fti, resultsToFeatures, None)
        }

        if (ranges.isEmpty) { EmptyPlan(strategy.filter, reduce) } else {
          // configure additional iterators based on the index
          // TODO pull this out to be SPI loaded so that new indices can be added seamlessly
          val indexIter = if (index.name == Z3Index.name) {
            strategy.values.toSeq.map { case v: Z3IndexValues =>
              val offset = index.keySpace.sharding.length + index.keySpace.sharing.length
              Z3Iterator.configure(v, offset, hints.getFilterCompatibility, ZIterPriority)
            }
          } else if (index.name == Z2Index.name) {
            strategy.values.toSeq.map { case v: Z2IndexValues =>
              Z2Iterator.configure(v, index.keySpace.sharding.length + index.keySpace.sharing.length, ZIterPriority)
            }
          } else if (index.name == S3Index.name) {
            strategy.values.toSeq.map { case v: S3IndexValues =>
              S3Iterator.configure(v, index.keySpace.sharding.length, ZIterPriority)
            }
          } else if (index.name == S2Index.name) {
            strategy.values.toSeq.map { case v: S2IndexValues =>
              S2Iterator.configure(v, index.keySpace.sharding.length, ZIterPriority)
            }
          } else {
            Seq.empty
          }

          // add the attribute-level vis iterator if necessary
          val visIter = index.sft.getVisibilityLevel match {
            case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(schema))
            case _ => Seq.empty
          }

          val iters = iter ++ indexIter ++ visIter

          val sort = hints.getSortFields
          val max = hints.getMaxFeatures
          val project = hints.getProjection

          BatchScanPlan(filter, tables, ranges, iters, colFamily, eToF, reduce, sort, max, project, numThreads)
        }
    }
  }

  override def createWriter(
      sft: SimpleFeatureType,
      indices: Seq[GeoMesaFeatureIndex[_, _]],
      partition: Option[String],
      atomic: Boolean): IndexWriter = {
    val wrapper = AccumuloWritableFeature.wrapper(sft, groups, indices)
    (atomic, sft.isVisibilityRequired) match {
      case (false, false) => new AccumuloIndexWriter(ds, indices, wrapper, partition)
      case (false, true)  => new AccumuloIndexWriter(ds, indices, wrapper, partition)  with RequiredVisibilityWriter
      case (true, false)  => new AccumuloAtomicIndexWriter(ds, sft, indices, wrapper, partition)
      case (true, true)   => new AccumuloAtomicIndexWriter(ds, sft, indices, wrapper, partition)  with RequiredVisibilityWriter
    }
  }
}

object AccumuloIndexAdapter {

  val ZIterPriority = 23

  /**
    * Accumulo entries to features
    *
    * @param _index index
    * @param _sft simple feature type
    */
  abstract class AccumuloResultsToFeatures(_index: GeoMesaFeatureIndex[_, _], _sft: SimpleFeatureType)
      extends IndexResultsToFeatures[Entry[Key, Value]](_index, _sft)

  object AccumuloResultsToFeatures {

    def apply(index: GeoMesaFeatureIndex[_, _], sft: SimpleFeatureType): AccumuloResultsToFeatures = {
      if (index.serializedWithId) {
        new AccumuloIndexWithIdResultsToFeatures(index, sft)
      } else {
        new AccumuloIndexResultsToFeatures(index, sft)
      }
    }

    /**
     * Set visibility in a feature based on the row key visibility
     *
     * @param sf feature
     * @param key row key
     */
    private def applyVisibility(sf: SimpleFeature, key: Key): Unit = {
      val visibility = key.getColumnVisibility
      if (visibility.getLength > 0) {
        SecurityUtils.setFeatureVisibility(sf, visibility.toString)
      }
    }

    class AccumuloIndexResultsToFeatures(_index: GeoMesaFeatureIndex[_, _], _sft: SimpleFeatureType)
        extends AccumuloResultsToFeatures(_index, _sft) {

      def this() = this(null, null) // no-arg constructor required for serialization

      override def apply(result: Entry[Key, Value]): SimpleFeature = {
        val row = result.getKey.getRow
        val id = index.getIdFromRow(row.getBytes, 0, row.getLength, null)
        val sf = serializer.deserialize(id, result.getValue.get)
        applyVisibility(sf, result.getKey)
        sf
      }
    }

    class AccumuloIndexWithIdResultsToFeatures(_index: GeoMesaFeatureIndex[_, _], _sft: SimpleFeatureType)
        extends AccumuloResultsToFeatures(_index, _sft) {

      def this() = this(null, null) // no-arg constructor required for serialization

      override def apply(result: Entry[Key, Value]): SimpleFeature = {
        val sf = serializer.deserialize(result.getValue.get)
        applyVisibility(sf, result.getKey)
        sf
      }
    }
  }
}
