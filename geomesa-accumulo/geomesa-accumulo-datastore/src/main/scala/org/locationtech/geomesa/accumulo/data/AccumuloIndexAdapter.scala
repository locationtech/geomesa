/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.Map.Entry

import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Key, Mutation, Range, Value}
import org.apache.accumulo.core.file.keyfunctor.RowFunctor
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloIndexAdapter.{AccumuloIndexWriter, AccumuloResultsToFeatures, ZIterPriority}
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan.{BatchScanPlan, EmptyPlan}
import org.locationtech.geomesa.accumulo.index.{AccumuloJoinIndex, JoinIndex}
import org.locationtech.geomesa.accumulo.iterators.ArrowIterator.AccumuloArrowResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator.AccumuloBinResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators.DensityIterator.AccumuloDensityResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators.StatsIterator.AccumuloStatsResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, TableUtils}
import org.locationtech.geomesa.index.api.IndexAdapter.BaseIndexWriter
import org.locationtech.geomesa.index.api.QueryPlan.IndexResultsToFeatures
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.{ColumnGroups, QueryHints}
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.s2.{S2Index, S2IndexValues}
import org.locationtech.geomesa.index.index.s3.{S3Index, S3IndexValues}
import org.locationtech.geomesa.index.index.z2.{XZ2Index, Z2Index, Z2IndexValues}
import org.locationtech.geomesa.index.index.z3.{XZ3Index, Z3Index, Z3IndexValues}
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.planning.LocalQueryRunner.{ArrowDictionaryHook, LocalTransformReducer}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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

    // even if the table existed, we still need to check the splits and locality groups if its shared
    if (created || index.keySpace.sharing.nonEmpty) {
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

      // enable block cache
      tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

      if (index.name == IdIndex.name) {
        // enable the row functor as the feature ID is stored in the Row ID
        tableOps.setProperty(table, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey, classOf[RowFunctor].getCanonicalName)
        tableOps.setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey, "true")
      }
    }
  }

  override def renameTable(from: String, to: String): Unit = {
    if (tableOps.exists(from)) {
      tableOps.rename(from, to)
    }
  }

  override def deleteTables(tables: Seq[String]): Unit = {
    tables.par.foreach { table =>
      if (tableOps.exists(table)) {
        tableOps.delete(table)
      }
    }
  }

  override def clearTables(tables: Seq[String], prefix: Option[Array[Byte]]): Unit = {
    val auths = ds.auths // get the auths once up front
    tables.par.foreach { table =>
      if (tableOps.exists(table)) {
        val config = GeoMesaBatchWriterConfig().setMaxWriteThreads(ds.config.writeThreads)
        WithClose(ds.connector.createBatchDeleter(table, auths, ds.config.queries.threads, config)) { deleter =>
          val range = prefix.map(p => Range.prefix(new Text(p))).getOrElse(new Range())
          deleter.setRanges(Collections.singletonList(range))
          deleter.delete()
        }
      }
    }
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
      (Some(new Text(AccumuloIndexAdapter.mapColumnFamily(index)(cf))), s)
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
      case i: AccumuloJoinIndex =>
        i.createQueryPlan(filter, tables, ranges, colFamily, schema, ecql, hints, numThreads)

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

  override def createWriter(sft: SimpleFeatureType,
                            indices: Seq[GeoMesaFeatureIndex[_, _]],
                            partition: Option[String]): AccumuloIndexWriter = {
    // make sure to provide our index values for attribute join indices if we need them
    val base = WritableFeature.wrapper(sft, groups)
    val wrapper = if (indices.exists(_.isInstanceOf[AccumuloJoinIndex])) {
      AccumuloWritableFeature.wrapper(sft, base)
    } else {
      base
    }
    new AccumuloIndexWriter(ds, indices, wrapper, partition)
  }
}

object AccumuloIndexAdapter {

  val ZIterPriority = 23

  /**
    * Set visibility in a feature based on the row key visibility
    *
    * @param sf feature
    * @param key row key
    */
  def applyVisibility(sf: SimpleFeature, key: Key): Unit = {
    val visibility = key.getColumnVisibility
    if (visibility.getLength > 0) {
      SecurityUtils.setFeatureVisibility(sf, visibility.toString)
    }
  }

  /**
    * Maps columns families from the default index implementation to the accumulo-specific values
    * that were used
    *
    * @param index feature index
    * @return
    */
  def mapColumnFamily(index: GeoMesaFeatureIndex[_, _]): Array[Byte] => Array[Byte] = {
    // last version before col families start matching up with index-api
    val flip = index.name match {
      case Z3Index.name  => 5
      case Z2Index.name  => 4
      case XZ3Index.name => 1
      case XZ2Index.name => 1
      case IdIndex.name  => 3
      case AttributeIndex.name | JoinIndex.name => 7
      case _ => 0
    }

    if (index.version > flip) {
      colFamily => colFamily
    } else if (index.version < 2 && index.name == IdIndex.name) {
      val bytes = "SFT".getBytes(StandardCharsets.UTF_8)
      _ => bytes
    } else if (index.version < 3 && (index.name == AttributeIndex.name || index.name == JoinIndex.name)) {
      _ => Array.empty
    } else if (index.name == JoinIndex.name) {
      val bytes = "I".getBytes(StandardCharsets.UTF_8)
      _ => bytes
    } else {
      val f = "F".getBytes(StandardCharsets.UTF_8)
      val a = "A".getBytes(StandardCharsets.UTF_8)
      colFamily => {
        if (java.util.Arrays.equals(colFamily, ColumnGroups.Default)) {
          f
        } else if (java.util.Arrays.equals(colFamily, ColumnGroups.Attributes)) {
          a
        } else {
          colFamily
        }
      }
    }
  }

  /**
    * Accumulo index writer implementation
    *
    * @param ds data store
    * @param indices indices to write to
    * @param wrapper feature wrapper
    * @param partition partition to write to (if partitioned schema)
    */
  class AccumuloIndexWriter(
      ds: AccumuloDataStore,
      indices: Seq[GeoMesaFeatureIndex[_, _]],
      wrapper: FeatureWrapper[WritableFeature],
      partition: Option[String]
    ) extends BaseIndexWriter[WritableFeature](indices, wrapper) {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    private val multiWriter = ds.connector.createMultiTableBatchWriter(GeoMesaBatchWriterConfig())
    private val writers = indices.toArray.map { index =>
      val table = index.getTableNames(partition) match {
        case Seq(t) => t // should always be writing to a single table here
        case tables => throw new IllegalStateException(s"Expected a single table but got: ${tables.mkString(", ")}")
      }
      multiWriter.getBatchWriter(table)
    }

    private val colFamilyMappings = indices.map(mapColumnFamily).toArray
    private val timestamps = indices.exists(i => !i.sft.isLogicalTime)

    private val defaultVisibility = new ColumnVisibility()
    private val visibilities = new java.util.HashMap[VisHolder, ColumnVisibility]()

    private var i = 0

    override protected def write(feature: WritableFeature, values: Array[RowKeyValue[_]], update: Boolean): Unit = {
      if (timestamps && update) {
        // for updates, ensure that our timestamps don't clobber each other
        multiWriter.flush()
        Thread.sleep(1)
      }
      i = 0
      while (i < values.length) {
        values(i) match {
          case kv: SingleRowKeyValue[_] =>
            val mutation = new Mutation(kv.row)
            kv.values.foreach { v =>
              val vis = if (v.vis.isEmpty) { defaultVisibility } else {
                val lookup = new VisHolder(v.vis)
                var cached = visibilities.get(lookup)
                if (cached == null) {
                  cached = new ColumnVisibility(v.vis)
                  visibilities.put(lookup, cached)
                }
                cached
              }
              mutation.put(colFamilyMappings(i)(v.cf), v.cq, vis, v.value)
            }
            writers(i).addMutation(mutation)

          case mkv: MultiRowKeyValue[_] =>
            mkv.rows.foreach { row =>
              val mutation = new Mutation(row)
              mkv.values.foreach { v =>
                val vis = if (v.vis.isEmpty) { defaultVisibility } else {
                  val lookup = new VisHolder(v.vis)
                  var cached = visibilities.get(lookup)
                  if (cached == null) {
                    cached = new ColumnVisibility(v.vis)
                    visibilities.put(lookup, cached)
                  }
                  cached
                }
                mutation.put(colFamilyMappings(i)(v.cf), v.cq, vis, v.value)
              }
              writers(i).addMutation(mutation)
            }
        }
        i += 1
      }
    }

    override protected def delete(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
      i = 0
      while (i < values.length) {
        values(i) match {
          case SingleRowKeyValue(row, _, _, _, _, _, vals) =>
            val mutation = new Mutation(row)
            vals.foreach { v =>
              val vis = if (v.vis.isEmpty) { defaultVisibility } else {
                val lookup = new VisHolder(v.vis)
                var cached = visibilities.get(lookup)
                if (cached == null) {
                  cached = new ColumnVisibility(v.vis)
                  visibilities.put(lookup, cached)
                }
                cached
              }
              mutation.putDelete(colFamilyMappings(i)(v.cf), v.cq, vis)
            }
            writers(i).addMutation(mutation)

          case MultiRowKeyValue(rows, _, _, _, _, _, vals) =>
            rows.foreach { row =>
              val mutation = new Mutation(row)
              vals.foreach { v =>
                val vis = if (v.vis.isEmpty) { defaultVisibility } else {
                  val lookup = new VisHolder(v.vis)
                  var cached = visibilities.get(lookup)
                  if (cached == null) {
                    cached = new ColumnVisibility(v.vis)
                    visibilities.put(lookup, cached)
                  }
                  cached
                }
                mutation.putDelete(colFamilyMappings(i)(v.cf), v.cq, vis)
              }
              writers(i).addMutation(mutation)
            }
        }
        i += 1
      }
    }

    override def flush(): Unit = multiWriter.flush()

    override def close(): Unit = multiWriter.close()
  }

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

    class AccumuloIndexResultsToFeatures(_index: GeoMesaFeatureIndex[_, _], _sft: SimpleFeatureType)
        extends AccumuloResultsToFeatures(_index, _sft) {

      def this() = this(null, null) // no-arg constructor required for serialization

      override def apply(result: Entry[Key, Value]): SimpleFeature = {
        val row = result.getKey.getRow
        val id = index.getIdFromRow(row.getBytes, 0, row.getLength, null)
        val sf = serializer.deserialize(id, result.getValue.get)
        AccumuloIndexAdapter.applyVisibility(sf, result.getKey)
        sf
      }
    }

    class AccumuloIndexWithIdResultsToFeatures(_index: GeoMesaFeatureIndex[_, _], _sft: SimpleFeatureType)
        extends AccumuloResultsToFeatures(_index, _sft) {

      def this() = this(null, null) // no-arg constructor required for serialization

      override def apply(result: Entry[Key, Value]): SimpleFeature = {
        val sf = serializer.deserialize(result.getValue.get)
        AccumuloIndexAdapter.applyVisibility(sf, result.getKey)
        sf
      }
    }
  }

  /**
    * Wrapper for byte array to use as a key in the cached visibilities map
    *
    * @param vis vis
    */
  class VisHolder(val vis: Array[Byte]) {

    override def equals(other: Any): Boolean = other match {
      case that: VisHolder => java.util.Arrays.equals(vis, that.vis)
      case _ => false
    }

    override def hashCode(): Int = java.util.Arrays.hashCode(vis)
  }
}
