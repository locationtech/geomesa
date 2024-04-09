/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data
package index

import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.api.IndexAdapter.{BaseIndexWriter, RequiredVisibilityWriter}
import org.locationtech.geomesa.index.api.QueryPlan.IndexResultsToFeatures
import org.locationtech.geomesa.index.api.WritableFeature.FeatureWrapper
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.planning.LocalQueryRunner
import org.locationtech.geomesa.index.planning.LocalQueryRunner.{ArrowDictionaryHook, LocalTransformReducer}
import org.locationtech.geomesa.redis.data.index.RedisAgeOff.AgeOffWriter
import org.locationtech.geomesa.redis.data.index.RedisIndexAdapter.{RedisIndexWriter, RedisResultsToFeatures}
import org.locationtech.geomesa.redis.data.index.RedisQueryPlan.{EmptyPlan, ZLexPlan}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.WithClose
import redis.clients.jedis.JedisPool

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/**
  * Index adapter for Redis
  *
  * @param ds data store
  */
class RedisIndexAdapter(ds: RedisDataStore) extends IndexAdapter[RedisDataStore] with StrictLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  // each 'table' is a sorted set - they are created automatically when you insert values
  override def createTable(
      index: GeoMesaFeatureIndex[_, _],
      partition: Option[String],
      splits: => Seq[Array[Byte]]): Unit = index.configureTableName(partition) // writes table name to metadata

  override def renameTable(from: String, to: String): Unit =
    WithClose(ds.connection.getResource)(_.renamenx(from, to))

  override def deleteTables(tables: Seq[String]): Unit =
    WithClose(ds.connection.getResource)(_.del(tables: _*))

  override def clearTables(tables: Seq[String], prefix: Option[Array[Byte]]): Unit = {
    require(prefix.isEmpty, "Prefix truncate is not supported")
    // we can delete the whole key, and it will automatically create it again on the next insert
    WithClose(ds.connection.getResource)(_.del(tables: _*))
  }

  override def createQueryPlan(strategy: QueryStrategy): RedisQueryPlan = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val QueryStrategy(filter, byteRanges, _, _, ecql, hints, _) = strategy

    val reducer = {
      val visible = Some(LocalQueryRunner.visible(Some(ds.config.authProvider)))
      val hook = Some(ArrowDictionaryHook(ds.stats, filter.filter))
      Some(new LocalTransformReducer(strategy.index.sft, ecql, visible, hints.getTransform, hints, hook))
    }

    if (byteRanges.isEmpty) { EmptyPlan(filter, reducer) } else {
      val tables = strategy.index.getTablesForQuery(filter.filter)
      val ranges = if (strategy.index.isInstanceOf[IdIndex]) {
        byteRanges.map(RedisIndexAdapter.toRedisIdRange)
      } else {
        byteRanges.map(RedisIndexAdapter.toRedisRange)
      }
      val results = new RedisResultsToFeatures(strategy.index, strategy.index.sft)
      val sort = hints.getSortFields
      val max = hints.getMaxFeatures
      val project = hints.getProjection

      ZLexPlan(filter, tables, ranges, ds.config.pipeline, ecql, results, reducer, sort, max, project)
    }
  }

  override def createWriter(
      sft: SimpleFeatureType,
      indices: Seq[GeoMesaFeatureIndex[_, _]],
      partition: Option[String],
      atomic: Boolean): RedisIndexWriter = {
    require(!atomic, "Redis data store does not currently support atomic writes")
    val aging = ds.aging.writer(sft)
    val wrapper = RedisWritableFeature.wrapper(sft)
    if (sft.isVisibilityRequired) {
      new RedisIndexWriter(ds.connection, indices, partition, aging, wrapper) with RequiredVisibilityWriter
    } else {
      new RedisIndexWriter(ds.connection, indices, partition, aging, wrapper)
    }
  }
}

object RedisIndexAdapter extends LazyLogging {

  val MinRange: Array[Byte] = "-".getBytes(StandardCharsets.UTF_8)
  val MaxRange: Array[Byte] = "+".getBytes(StandardCharsets.UTF_8)

  val InclusiveRangePrefix: Byte = "[".getBytes(StandardCharsets.UTF_8).head
  val ExclusiveRangePrefix: Byte = "(".getBytes(StandardCharsets.UTF_8).head

  /**
    * Convert a byte range to a redis zlex range
    *
    * @param byteRange geomesa range
    * @return
    */
  private def toRedisRange(byteRange: ByteRange): BoundedByteRange = byteRange match {
    case BoundedByteRange(start, end) =>
      val rangeStart = if (start.isEmpty) { RedisIndexAdapter.MinRange } else {
        val range = Array.ofDim[Byte](start.length + 1)
        System.arraycopy(start, 0, range, 1, start.length)
        range(0) = RedisIndexAdapter.InclusiveRangePrefix
        range
      }
      val rangeEnd = if (end.isEmpty) { RedisIndexAdapter.MaxRange } else {
        val range = Array.ofDim[Byte](end.length + 1)
        System.arraycopy(end, 0, range, 1, end.length)
        range(0) = RedisIndexAdapter.ExclusiveRangePrefix
        range
      }
      BoundedByteRange(rangeStart, rangeEnd)

    case SingleRowByteRange(row) =>
      val rangeStart = Array.ofDim[Byte](row.length + 1)
      System.arraycopy(row, 0, rangeStart, 1, row.length)
      rangeStart(0) = RedisIndexAdapter.InclusiveRangePrefix
      // since the value is appended to the row, we have to add a suffix
      val rangeEnd = Array.ofDim[Byte](row.length + 1 + ByteRange.UnboundedUpperRange.length)
      System.arraycopy(row, 0, rangeEnd, 1, row.length)
      System.arraycopy(ByteRange.UnboundedUpperRange, 0, rangeEnd, row.length + 1, ByteRange.UnboundedUpperRange.length)
      rangeEnd(0) = RedisIndexAdapter.ExclusiveRangePrefix
      BoundedByteRange(rangeStart, rangeEnd)
  }

  /**
    * Convert a byte range into a zlex range, specifically for the id index. Since we store the length
    * of the id in the row key, we have to prepend that to our ranges
    *
    * @param byteRange geomesa range
    * @return
    */
  private def toRedisIdRange(byteRange: ByteRange): BoundedByteRange = byteRange match {
    case BoundedByteRange(start, end) =>
      val rangeStart = if (start.isEmpty) { RedisIndexAdapter.MinRange } else {
        // add the two byte length prefix
        val range = Array.ofDim[Byte](start.length + 3)
        System.arraycopy(start, 0, range, 3, start.length)
        ByteArrays.writeShort(start.length.toShort, range, 1)
        range(0) = RedisIndexAdapter.InclusiveRangePrefix
        range
      }
      val rangeEnd = if (end.isEmpty) { RedisIndexAdapter.MaxRange } else {
        // add the two byte length prefix
        val range = Array.ofDim[Byte](end.length + 3)
        System.arraycopy(end, 0, range, 3, end.length)
        ByteArrays.writeShort(end.length.toShort, range, 1)
        range(0) = RedisIndexAdapter.ExclusiveRangePrefix
        range
      }
      BoundedByteRange(rangeStart, rangeEnd)

    case SingleRowByteRange(row) =>
      // add the two byte length prefix
      val rangeStart = Array.ofDim[Byte](row.length + 3)
      System.arraycopy(row, 0, rangeStart, 3, row.length)
      ByteArrays.writeShort(row.length.toShort, rangeStart, 1)
      rangeStart(0) = RedisIndexAdapter.InclusiveRangePrefix
      // since the value is appended to the row, we have to add a suffix
      val rangeEnd = Array.ofDim[Byte](row.length + 3 + ByteRange.UnboundedUpperRange.length)
      System.arraycopy(row, 0, rangeEnd, 3, row.length)
      System.arraycopy(ByteRange.UnboundedUpperRange, 0, rangeEnd, row.length + 3, ByteRange.UnboundedUpperRange.length)
      ByteArrays.writeShort(row.length.toShort, rangeEnd, 1)
      rangeEnd(0) = RedisIndexAdapter.ExclusiveRangePrefix
      BoundedByteRange(rangeStart, rangeEnd)
  }

  class RedisResultsToFeatures(_index: GeoMesaFeatureIndex[_, _], _sft: SimpleFeatureType)
      extends IndexResultsToFeatures[Array[Byte]](_index, _sft) {

    private var idSerializer: (Array[Byte], Int, Int, SimpleFeature) => String = _

    override def apply(result: Array[Byte]): SimpleFeature = {
      // parse out the feature id and the serialized value from the concatenated row + value
      val idStart = index.getIdOffset(result, 0, result.length)
      val idLength = ByteArrays.readShort(result, idStart)
      val id = idSerializer(result, idStart + 2, idLength, null)
      val valueStart = idStart + idLength + 2
      serializer.deserialize(id, result, valueStart, result.length - valueStart)
    }

    override protected def createSerializer: KryoFeatureSerializer = {
      idSerializer = GeoMesaFeatureIndex.idFromBytes(index.sft)
      KryoFeatureSerializer.builder(index.sft).`lazy`.withUserData.withoutId.build()
    }
  }

  /**
    * Writer for redis
    *
    * @param jedis connection
    * @param indices indices to write to
    * @param partition partition to write to
    * @param aging age-off writer
    * @param wrapper feature wrapper
    */
  class RedisIndexWriter(
      jedis: JedisPool,
      indices: Seq[GeoMesaFeatureIndex[_, _]],
      partition: Option[String],
      aging: Option[AgeOffWriter],
      wrapper: FeatureWrapper[RedisWritableFeature]
    ) extends BaseIndexWriter[RedisWritableFeature](indices, wrapper) {

    private val batchSize = RedisSystemProperties.WriteBatchSize.toInt match {
      case Some(s) if s > 0 => s - 1
      case _ =>
        throw new IllegalStateException(s"Value '${RedisSystemProperties.WriteBatchSize.get}' for " +
            s"'${RedisSystemProperties.WriteBatchSize.property}' is not a positive int")
    }

    private val tables = indices.toArray.map { index =>
      index.getTableNames(partition) match {
        case Seq(t) => t.getBytes(StandardCharsets.UTF_8) // should always be writing to a single table here
        case names => throw new IllegalStateException(s"Expected a single table but got: ${names.mkString(", ")}")
      }
    }

    private val inserts = Array.fill[java.util.Map[Array[Byte], java.lang.Double]](tables.length)(new java.util.HashMap[Array[Byte], java.lang.Double]())
    private val deletes = Array.fill[ArrayBuffer[Array[Byte]]](tables.length)(ArrayBuffer.empty[Array[Byte]])

    private var i = 0
    private var batch = 0

    private val errors = ArrayBuffer.empty[Throwable]

    override protected def append(feature: RedisWritableFeature, values: Array[RowKeyValue[_]]): Unit = {
      i = 0
      while (i < values.length) {
        val insert = inserts(i)
        values(i) match {
          case kv: SingleRowKeyValue[_] =>
            kv.values.foreach(v => insert.put(ByteArrays.concat(kv.row, v.value), 0d))
          case kv: MultiRowKeyValue[_] =>
            kv.rows.foreach(row => kv.values.foreach(v => insert.put(ByteArrays.concat(row, v.value), 0d)))
        }
        i += 1
      }

      aging.foreach(_.write(feature))

      if (batch < batchSize) {
        batch += 1
      } else {
        flush()
      }
    }

    override protected def update(
        feature: RedisWritableFeature,
        values: Array[RowKeyValue[_]],
        previous: RedisWritableFeature,
        previousValues: Array[RowKeyValue[_]]): Unit = {
      delete(previous, previousValues)
      append(feature, values)
    }

    override protected def delete(feature: RedisWritableFeature, values: Array[RowKeyValue[_]]): Unit = {
      i = 0
      while (i < values.length) {
        val buffer = deletes(i)
        values(i) match {
          case kv: SingleRowKeyValue[_] =>
            kv.values.foreach(v => buffer.append(ByteArrays.concat(kv.row, v.value)))
          case kv: MultiRowKeyValue[_] =>
            kv.rows.foreach(row => kv.values.foreach(v => buffer.append(ByteArrays.concat(row, v.value))))
        }
        i += 1
      }

      aging.foreach(_.delete(feature))

      if (batch < batchSize) {
        batch += 1
      } else {
        flush()
      }
    }

    override def flush(): Unit = {
      i = 0
      while (i < tables.length) {
        if (deletes(i).nonEmpty) {
          try { WithClose(jedis.getResource)(_.zrem(tables(i), deletes(i).toSeq: _*)) } catch {
            case NonFatal(e) => errors.append(e)
          }
          deletes(i).clear()
        }
        if (!inserts(i).isEmpty) {
          try { WithClose(jedis.getResource)(_.zadd(tables(i), inserts(i))) } catch {
            case NonFatal(e) => errors.append(e)
          }
          inserts(i).clear()
        }
        i += 1
      }
      batch = 0

      try { aging.foreach(_.flush()) } catch {
        case NonFatal(e) => errors.append(e)
      }

      throwErrors()
    }

    override def close(): Unit = {
      try { flush() } catch {
        case NonFatal(e) => errors.append(e)
      }
      try { aging.foreach(_.close()) } catch {
        case NonFatal(e) => errors.append(e)
      }
      throwErrors()
    }

    private def throwErrors(): Unit = {
      if (errors.nonEmpty) {
        val error = errors.head
        errors.tail.foreach(error.addSuppressed)
        errors.clear()
        throw error
      }
    }
  }
}
