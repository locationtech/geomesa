/***********************************************************************
 * Copyright (c) 2017-2019 IBM
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import com.datastax.driver.core.Row
import com.datastax.driver.core.exceptions.AlreadyExistsException
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import com.typesafe.scalalogging.StrictLogging
import org.locationtech.geomesa.cassandra.ColumnSelect
import org.locationtech.geomesa.cassandra.data.CassandraIndexAdapter.CassandraIndexWriter
import org.locationtech.geomesa.cassandra.index.CassandraColumnMapper
import org.locationtech.geomesa.cassandra.index.CassandraColumnMapper.{FeatureIdColumnName, SimpleFeatureColumnName}
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.api.IndexAdapter.IndexWriter
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.planning.LocalQueryRunner
import org.locationtech.geomesa.index.planning.LocalQueryRunner.ArrowDictionaryHook
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class CassandraIndexAdapter(ds: CassandraDataStore) extends IndexAdapter[CassandraDataStore] with StrictLogging {

  override val tableNameLimit: Option[Int] = Some(CassandraIndexAdapter.TableNameLimit)

  override def createTable(
      index: GeoMesaFeatureIndex[_, _],
      partition: Option[String],
      splits: => Seq[Array[Byte]]): Unit = {
    val cluster = ds.session.getCluster
    val table = index.configureTableName(partition, tableNameLimit) // writes metadata for table name

    if (cluster.getMetadata.getKeyspace(ds.session.getLoggedKeyspace).getTable(table) == null) {
      val columns = CassandraColumnMapper(index).columns
      val (partitions, pks) = columns.partition(_.partition)
      val create = s"CREATE TABLE $table (${columns.map(c => s"${c.name} ${c.cType}").mkString(", ")}, " +
          s"PRIMARY KEY (${partitions.map(_.name).mkString("(", ", ", ")")}" +
          s"${if (pks.nonEmpty) { pks.map(_.name).mkString(", ", ", ", "")} else { "" }}))"
      logger.debug(create)
      try { ds.session.execute(create) } catch {
        case _: AlreadyExistsException => // ignore, another thread created it for us
      }
    }
  }

  override def deleteTables(tables: Seq[String]): Unit = {
    tables.foreach { table =>
      val delete = s"DROP TABLE IF EXISTS $table"
      logger.debug(delete)
      ds.session.execute(delete)
    }
  }

  override def clearTables(tables: Seq[String], prefix: Option[Array[Byte]]): Unit = {
    if (prefix.isDefined) {
      throw new IllegalArgumentException("Received a range in `clearTables` but table sharing should be disabled")
    }
    tables.foreach { table =>
      val truncate = s"TRUNCATE $table"
      logger.debug(truncate)
      ds.session.execute(truncate)
    }
  }

  override def createQueryPlan(strategy: QueryStrategy): CassandraQueryPlan = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val QueryStrategy(filter, _, keyRanges, tieredKeyRanges, ecql, hints, _) = strategy

    val serializer = KryoFeatureSerializer(strategy.index.sft, SerializationOptions.builder.`lazy`.withoutId.build)
    val idSerializer = GeoMesaFeatureIndex.idFromBytes(strategy.index.sft)

    val hook = Some(ArrowDictionaryHook(ds.stats, filter.filter))
    val transform: CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature] =
      LocalQueryRunner.transform(strategy.index.sft, _, hints.getTransform, hints, hook)

    val rowsToFeatures = (rows: CloseableIterator[Row]) => {
      val features = rows.map { row =>
        val fid = {
          val bytes = row.get(FeatureIdColumnName, classOf[String]).getBytes(StandardCharsets.UTF_8)
          idSerializer.apply(bytes, 0, bytes.length, null)
        }
        val sf = row.getBytes(SimpleFeatureColumnName)
        val bytes = Array.ofDim[Byte](sf.limit())
        sf.get(bytes)
        serializer.deserialize(fid, bytes)
      }
      ecql match {
        case None    => transform(features)
        case Some(e) => transform(features.filter(e.evaluate))
      }
    }

    if (keyRanges.isEmpty) { EmptyPlan(filter, rowsToFeatures) } else {
      val ks = ds.session.getLoggedKeyspace
      val mapper = CassandraColumnMapper(strategy.index)
      val ranges = keyRanges.flatMap(mapper.select(_, tieredKeyRanges))
      val tables = strategy.index.getTablesForQuery(filter.filter)
      val statements = tables.flatMap(table => ranges.map(r => CassandraIndexAdapter.statement(ks, table, r.clauses)))
      StatementPlan(filter, tables, statements, ds.config.queryThreads, ecql, rowsToFeatures)
    }
  }

  override def createWriter(sft: SimpleFeatureType,
                            indices: Seq[GeoMesaFeatureIndex[_, _]],
                            partition: Option[String]): CassandraIndexWriter =
    new CassandraIndexWriter(ds, indices, WritableFeature.wrapper(sft, groups), partition)
}

object CassandraIndexAdapter {

  val TableNameLimit = 48

  def statement(keyspace: String, table: String, criteria: Seq[ColumnSelect]): Select = {
    val select = QueryBuilder.select.all.from(keyspace, table)
    criteria.foreach { c =>
      if (c.start == null) {
        if (c.end != null) {
          if (c.endInclusive) {
            select.where(QueryBuilder.lte(c.column.name, c.end))
          } else {
            select.where(QueryBuilder.lt(c.column.name, c.end))
          }
        }
      } else if (c.end == null) {
        if (c.startInclusive) {
          select.where(QueryBuilder.gte(c.column.name, c.start))
        } else {
          select.where(QueryBuilder.gt(c.column.name, c.start))
        }
      } else if (c.start == c.end) {
        select.where(QueryBuilder.eq(c.column.name, c.start))
      } else {
        if (c.startInclusive) {
          select.where(QueryBuilder.gte(c.column.name, c.start))
        } else {
          select.where(QueryBuilder.gt(c.column.name, c.start))
        }
        if (c.endInclusive) {
          select.where(QueryBuilder.lte(c.column.name, c.end))
        } else {
          select.where(QueryBuilder.lt(c.column.name, c.end))
        }
      }
    }
    select
  }

  class CassandraIndexWriter(ds: CassandraDataStore,
                             indices: Seq[GeoMesaFeatureIndex[_, _]],
                             wrapper: FeatureWrapper,
                             partition: Option[String]) extends IndexWriter(indices, wrapper) with StrictLogging {

    private val mappers = indices.toArray.map { index =>
      val mapper = CassandraColumnMapper(index)
      val table = index.getTableNames(partition) match {
        case Seq(t) => t // should always be writing to a single table here
        case tables => throw new IllegalStateException(s"Expected a single table but got: ${tables.mkString(", ")}")
      }
      val insert = mapper.insert(ds.session, table)
      val delete = mapper.delete(ds.session, table)
      (mapper, insert, delete)
    }

    private var i = 0

    override protected def write(feature: WritableFeature, values: Array[RowKeyValue[_]], update: Boolean): Unit = {
      i = 0
      while (i < values.length) {
        val (mapper, statement, _) = mappers(i)
        values(i) match {
          case kv: SingleRowKeyValue[_] =>
            val bindings = mapper.bind(kv)
            logger.trace(s"${statement.getQueryString} : ${debug(bindings)}")
            ds.session.execute(statement.bind(bindings: _*))

          case mkv: MultiRowKeyValue[_] =>
            mkv.split.foreach { kv =>
              val bindings = mapper.bind(kv)
              logger.trace(s"${statement.getQueryString} : ${debug(bindings)}")
              ds.session.execute(statement.bind(bindings: _*))
            }
        }
        i += 1
      }
    }

    override protected def delete(feature: WritableFeature, values: Array[RowKeyValue[_]]): Unit = {
      i = 0
      while (i < values.length) {
        val (mapper, _, statement) = mappers(i)
        values(i) match {
          case kv: SingleRowKeyValue[_] =>
            val bindings = mapper.bindDelete(kv)
            logger.trace(s"${statement.getQueryString} : ${debug(bindings)}")
            ds.session.execute(statement.bind(bindings: _*))

          case mkv: MultiRowKeyValue[_] =>
            mkv.split.foreach { kv =>
              val bindings = mapper.bindDelete(kv)
              logger.trace(s"${statement.getQueryString} : ${debug(bindings)}")
              ds.session.execute(statement.bind(bindings: _*))
            }
        }
        i += 1
      }
    }

    override def flush(): Unit = {}
    override def close(): Unit = {}
  }

  private def debug(bindings: Seq[AnyRef]): String =
    bindings.map {
      case null => "null"
      case b: ByteBuffer if b.hasArray => ByteArrays.toHex(b.array(), b.arrayOffset(), b.limit())
      case b => b.toString
    }.mkString(",")
}
