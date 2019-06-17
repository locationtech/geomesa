/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

import com.google.common.primitives.UnsignedBytes
import org.geotools.data.Query
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.{KryoFeatureSerializer, ProjectingKryoFeatureDeserializer}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.TestGeoMesaDataStore._
import org.locationtech.geomesa.index.api.IndexAdapter.IndexWriter
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api.{WritableFeature, _}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata
import org.locationtech.geomesa.index.stats.MetadataBackedStats.WritableStat
import org.locationtech.geomesa.index.stats._
import org.locationtech.geomesa.index.utils.{Explainer, LocalLocking}
import org.locationtech.geomesa.utils.audit.{AuditProvider, AuditWriter}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.stats.Stat
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.SortedSet

class TestGeoMesaDataStore(looseBBox: Boolean)
    extends GeoMesaDataStore[TestGeoMesaDataStore](TestConfig(looseBBox)) with LocalLocking {

  override val metadata: GeoMesaMetadata[String] = new InMemoryMetadata[String]

  override val adapter: TestIndexAdapter = new TestIndexAdapter

  override val stats: GeoMesaStats = new TestStats(this, new InMemoryMetadata[Stat]())

  override def getQueryPlan(query: Query, index: Option[String], explainer: Explainer): Seq[TestQueryPlan] =
    super.getQueryPlan(query, index, explainer).asInstanceOf[Seq[TestQueryPlan]]
}

object TestGeoMesaDataStore {

  class TestIndexAdapter extends IndexAdapter[TestGeoMesaDataStore] {

    import ByteArrays.ByteOrdering

    private val ordering = new Ordering[SingleRowKeyValue[_]] {
      override def compare(x: SingleRowKeyValue[_], y: SingleRowKeyValue[_]): Int = ByteOrdering.compare(x.row, y.row)
    }

    private val tables = scala.collection.mutable.Map.empty[String, scala.collection.mutable.SortedSet[SingleRowKeyValue[_]]]

    override def createTable(
        index: GeoMesaFeatureIndex[_, _],
        partition: Option[String],
        splits: => Seq[Array[Byte]]): Unit = {
      val table = index.configureTableName(partition) // writes table name to metadata
      tables.put(table, scala.collection.mutable.SortedSet.empty[SingleRowKeyValue[_]](ordering))
    }

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
      val ranges = strategy.ranges.map {
        case SingleRowByteRange(row)  => TestRange(row, ByteArrays.rowFollowingRow(row))
        case BoundedByteRange(lo, hi) => TestRange(lo, hi)
      }
      val opts = if (strategy.index.serializedWithId) { SerializationOptions.none } else { SerializationOptions.withoutId }
      val transform = strategy.hints.getTransformSchema
      val serializer = transform match {
        case None    => KryoFeatureSerializer(strategy.index.sft, opts)
        case Some(s) => new ProjectingKryoFeatureDeserializer(strategy.index.sft, s, opts)
      }
      val ecql = strategy.ecql.map(FastFilterFactory.optimize(transform.getOrElse(strategy.index.sft), _))
      TestQueryPlan(strategy.filter, tables, serializer, ranges, ecql)
    }

    override def createWriter(sft: SimpleFeatureType,
                              indices: Seq[GeoMesaFeatureIndex[_, _]],
                              partition: Option[String]): IndexWriter = {
      val tables = indices.map(i => this.tables(i.getTableNames(partition).head))
      new TestIndexWriter(indices, WritableFeature.wrapper(sft, groups), tables)
    }

    override def toString: String = getClass.getSimpleName
  }

  case class TestQueryPlan(filter: FilterStrategy,
                           tables: scala.collection.Map[String, SortedSet[SingleRowKeyValue[_]]],
                           serializer: SimpleFeatureSerializer,
                           ranges: Seq[TestRange],
                           ecql: Option[Filter]) extends QueryPlan[TestGeoMesaDataStore] {
    override def scan(ds: TestGeoMesaDataStore): CloseableIterator[SimpleFeature] = {
      def contained(range: TestRange, row: Array[Byte]): Boolean =
        ByteArrays.ByteOrdering.compare(range.start, row) <= 0 &&
            (range.end.isEmpty || ByteArrays.ByteOrdering.compare(range.end, row) > 0)

      val names = filter.index.getTableNames(None)
      val tbls = names.flatMap(tables.apply)
      val matches = tbls.flatMap { kv =>
        if (!ranges.exists(contained(_, kv.row))) {
          Iterator.empty
        } else {
          kv.values.iterator.flatMap { value =>
            val feature = {
              val sf = serializer.deserialize(value.value).asInstanceOf[ScalaSimpleFeature]
              sf.setId(filter.index.getIdFromRow(kv.row, 0, kv.row.length, sf))
              sf
            }
            if (ecql.forall(_.evaluate(feature))) {
              Iterator.single(feature)
            } else {
              Iterator.empty
            }
          }
        }
      }
      matches.iterator
    }

    override def explain(explainer: Explainer, prefix: String): Unit = {
      explainer(s"ranges (${ranges.length}): ${ranges.take(5).map(r =>
        s"[${r.start.map(UnsignedBytes.toString).mkString(";")}::" +
            s"${r.end.map(UnsignedBytes.toString).mkString(";")})").mkString(",")}")
      explainer(s"ecql: ${ecql.map(org.locationtech.geomesa.filter.filterToString).getOrElse("INCLUDE")}")
    }
  }

  case class TestRange(start: Array[Byte], end: Array[Byte]) {
    override def toString: String = s"TestRange(${start.mkString(":")}, ${end.mkString(":")}}"
  }

  class TestIndexWriter(indices: Seq[GeoMesaFeatureIndex[_, _]],
                        wrapper: FeatureWrapper,
                        tables: Seq[scala.collection.mutable.SortedSet[SingleRowKeyValue[_]]])
      extends IndexWriter(indices, wrapper) {

    private var i = 0

    override protected def write(feature: WritableFeature, values: Array[RowKeyValue[_]], update: Boolean): Unit = {
      i = 0
      values.foreach {
        case kv: SingleRowKeyValue[_] => tables(i).add(kv); i += 1
        case kv: MultiRowKeyValue[_] => kv.split.foreach(tables(i).add); i += 1
      }
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
    override val audit: Option[(AuditWriter, AuditProvider, String)] = None
    override val generateStats: Boolean = true
    override val queryThreads: Int = 1
    override val queryTimeout: Option[Long] = None
    override val caching: Boolean = false
    override val namespace: Option[String] = None
  }

  class TestStats(ds: TestGeoMesaDataStore, metadata: GeoMesaMetadata[Stat])
      extends MetadataBackedStats(ds, metadata, true) {
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
