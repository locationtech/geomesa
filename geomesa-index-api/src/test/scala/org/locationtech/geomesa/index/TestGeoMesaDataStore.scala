/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

import org.geotools.api.data.Query
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.TestGeoMesaDataStore._
import org.locationtech.geomesa.index.api.IndexAdapter.{BaseIndexWriter, IndexWriter, RequiredVisibilityWriter}
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{DataStoreQueryConfig, GeoMesaDataStoreConfig}
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata
import org.locationtech.geomesa.index.planning.LocalQueryRunner.{ArrowDictionaryHook, LocalTransformReducer}
import org.locationtech.geomesa.index.stats.MetadataBackedStats.WritableStat
import org.locationtech.geomesa.index.stats._
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.index.utils.{Explainer, LocalLocking}
import org.locationtech.geomesa.security.DefaultAuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditProvider, AuditWriter}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.stats.Stat

import scala.collection.SortedSet

class TestGeoMesaDataStore(looseBBox: Boolean)
    extends GeoMesaDataStore[TestGeoMesaDataStore](TestConfig(looseBBox)) with LocalLocking {

  override val metadata: GeoMesaMetadata[String] = new InMemoryMetadata[String]

  override val adapter: TestIndexAdapter = new TestIndexAdapter(this)

  override val stats: GeoMesaStats = new TestStats(this, new InMemoryMetadata[Stat]())

  override def getQueryPlan(query: Query, index: Option[String], explainer: Explainer): Seq[TestQueryPlan] =
    super.getQueryPlan(query, index, explainer).asInstanceOf[Seq[TestQueryPlan]]
}

object TestGeoMesaDataStore {

  class TestIndexAdapter(ds: TestGeoMesaDataStore) extends IndexAdapter[TestGeoMesaDataStore] {

    import ByteArrays.ByteOrdering
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    private val ordering = new Ordering[SingleRowKeyValue[_]] {
      override def compare(x: SingleRowKeyValue[_], y: SingleRowKeyValue[_]): Int = ByteOrdering.compare(x.row, y.row)
    }

    private val tables = scala.collection.mutable.Map.empty[String, scala.collection.mutable.SortedSet[SingleRowKeyValue[_]]]

    override def createTable(
        index: GeoMesaFeatureIndex[_, _],
        partition: Option[String],
        splits: => Seq[Array[Byte]]): Unit = {
      val table = index.configureTableName(partition) // writes table name to metadata
      if (!tables.contains(table)) {
        tables.put(table, scala.collection.mutable.SortedSet.empty[SingleRowKeyValue[_]](ordering))
      }
    }

    override def renameTable(from: String, to: String): Unit =
      tables.remove(from).foreach(tables.put(to, _))

    override def deleteTables(tables: Seq[String]): Unit = tables.foreach(this.tables.remove)

    override def clearTables(tables: Seq[String], prefix: Option[Array[Byte]]): Unit = {
      val predicate: Option[Array[Byte] => Boolean] = prefix.map(p => row => row.startsWith(p))
      tables.map(this.tables.apply).foreach { table =>
        predicate match {
          case None    => table.clear()
          case Some(p) => table.filter(r => p(r.row)).foreach(table.remove)
        }
      }
    }

    override def createQueryPlan(strategy: QueryStrategy): QueryPlan[TestGeoMesaDataStore] = {
      import org.locationtech.geomesa.index.conf.QueryHints.RichHints

      val tables = strategy.index.getTablesForQuery(strategy.filter.filter).flatMap(t => this.tables.get(t).map(t -> _))
      val ranges = strategy.ranges.map {
        case SingleRowByteRange(row)  => TestRange(row, ByteArrays.rowFollowingRow(row))
        case BoundedByteRange(lo, hi) => TestRange(lo, hi)
      }
      val opts = if (strategy.index.serializedWithId) { SerializationOptions.none } else { SerializationOptions.withoutId }
      val serializer = KryoFeatureSerializer(strategy.index.sft, opts)
      val ecql = strategy.ecql.map(FastFilterFactory.optimize(strategy.index.sft, _))
      val transform = strategy.hints.getTransform
      val reducer = {
        val arrowHook = Some(ArrowDictionaryHook(ds.stats, strategy.filter.filter))
        Some(new LocalTransformReducer(strategy.index.sft, ecql, None, transform, strategy.hints, arrowHook))
      }
      val maxFeatures = strategy.hints.getMaxFeatures
      val sort = strategy.hints.getSortFields
      val project = strategy.hints.getProjection

      TestQueryPlan(strategy.filter, tables.toMap, strategy.index.sft, serializer, ranges, reducer, ecql, sort, maxFeatures, project)
    }

    override def createWriter(
        sft: SimpleFeatureType,
        indices: Seq[GeoMesaFeatureIndex[_, _]],
        partition: Option[String],
        atomic: Boolean): IndexWriter = {
      require(!atomic, "Test data store does not currently support atomic writes")
      val tables = indices.map(i => this.tables(i.getTableNames(partition).head))
      val wrapper = WritableFeature.wrapper(sft, groups)
      if (sft.isVisibilityRequired) {
        new TestIndexWriter(indices, wrapper, tables) with RequiredVisibilityWriter
      } else {
        new TestIndexWriter(indices, wrapper, tables)
      }
    }

    override def toString: String = getClass.getSimpleName
  }

  case class TestQueryPlan(
      filter: FilterStrategy,
      tables: Map[String, SortedSet[SingleRowKeyValue[_]]],
      sft: SimpleFeatureType,
      serializer: SimpleFeatureSerializer,
      ranges: Seq[TestRange],
      reducer: Option[FeatureReducer],
      ecql: Option[Filter],
      sort: Option[Seq[(String, Boolean)]],
      maxFeatures: Option[Int],
      projection: Option[QueryReferenceSystems]
    ) extends QueryPlan[TestGeoMesaDataStore] {

    override type Results = SimpleFeature

    override val resultsToFeatures: ResultsToFeatures[SimpleFeature] = ResultsToFeatures.identity(sft)

    override def scan(ds: TestGeoMesaDataStore): CloseableIterator[SimpleFeature] = {
      def contained(range: TestRange, row: Array[Byte]): Boolean =
        ByteArrays.ByteOrdering.compare(range.start, row) <= 0 &&
            (range.end.isEmpty || ByteArrays.ByteOrdering.compare(range.end, row) > 0)

      val matches = tables.values.flatten.flatMap { kv =>
        if (!ranges.exists(contained(_, kv.row))) {
          Iterator.empty
        } else {
          kv.values.iterator.map { value =>
            val sf = serializer.deserialize(value.value).asInstanceOf[ScalaSimpleFeature]
            sf.setId(filter.index.getIdFromRow(kv.row, 0, kv.row.length, sf))
            sf
          }
        }
      }
      matches.iterator
    }

    override def explain(explainer: Explainer, prefix: String): Unit = {
      explainer(s"tables: ${tables.keys.mkString(", ")}")
      explainer(s"ranges (${ranges.length}): ${ranges.take(5).map(r =>
        s"[${r.start.map(ByteArrays.toHex).mkString(";")}::" +
            s"${r.end.map(ByteArrays.toHex).mkString(";")})").mkString(",")}")
      explainer(s"ecql: ${ecql.map(org.locationtech.geomesa.filter.filterToString).getOrElse("INCLUDE")}")
    }
  }

  case class TestRange(start: Array[Byte], end: Array[Byte]) {
    override def toString: String = s"TestRange(${start.mkString(":")}, ${end.mkString(":")}}"
  }

  class TestIndexWriter(
      indices: Seq[GeoMesaFeatureIndex[_, _]],
      wrapper: FeatureWrapper[WritableFeature],
      tables: Seq[scala.collection.mutable.SortedSet[SingleRowKeyValue[_]]]
    ) extends BaseIndexWriter[WritableFeature](indices, wrapper) {

    private var i = 0

    override protected def append(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
      i = 0
      values.foreach {
        case kv: SingleRowKeyValue[_] => tables(i).add(kv); i += 1
        case kv: MultiRowKeyValue[_] => kv.split.foreach(tables(i).add); i += 1
      }
    }

    override protected def update(
        feature: WritableFeature,
        values: Array[RowKeyValue[_]],
        previous: WritableFeature,
        previousValues: Array[RowKeyValue[_]]): Unit = {
      delete(previous, previousValues)
      append(feature, values)
    }

    override protected def delete(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
      i = 0
      values.foreach {
        case kv: SingleRowKeyValue[_] => tables(i).remove(kv); i += 1
        case kv: MultiRowKeyValue[_] => kv.split.foreach(tables(i).remove); i += 1
      }
    }

    override def flush(): Unit = {}
    override def close(): Unit = {}
  }

  case class TestConfig(looseBBox: Boolean) extends GeoMesaDataStoreConfig {
    override val catalog: String = "test"
    override val authProvider = new DefaultAuthorizationsProvider()
    override val audit: Option[(AuditWriter, AuditProvider, String)] = None
    override val generateStats: Boolean = true
    override val queries: DataStoreQueryConfig = new DataStoreQueryConfig() {
      override val threads: Int = 1
      override val timeout: Option[Long] = None
      override def looseBBox: Boolean = TestConfig.this.looseBBox
      override def parallelPartitionScans: Boolean = false
    }
    override val namespace: Option[String] = None
  }

  class TestStats(ds: TestGeoMesaDataStore, metadata: GeoMesaMetadata[Stat])
      extends MetadataBackedStats(ds, metadata) {
    override protected def write(typeName: String, stats: Seq[WritableStat]): Unit = {
      synchronized {
        stats.foreach { case WritableStat(key, stat, merge) =>
          if (merge) {
            metadata.insert(typeName, key, metadata.read(typeName, key, cache = false).map(_ + stat).getOrElse(stat))
          } else {
            metadata.insert(typeName, key, stat)
          }
        }
      }
    }
  }
}
