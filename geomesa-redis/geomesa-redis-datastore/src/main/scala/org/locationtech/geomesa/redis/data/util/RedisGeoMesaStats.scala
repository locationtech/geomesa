/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.redis.data
package util

import org.locationtech.geomesa.index.stats.MetadataBackedStats.{StatsMetadataSerializer, WritableStat}
import org.locationtech.geomesa.index.stats.{MetadataBackedStats, Stat}
import org.locationtech.geomesa.redis.data.util.RedisGeoMesaStats.RedisStat
import org.locationtech.geomesa.utils.io.WithClose

import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.util.control.NonFatal

/**
  * Redis stats implementation
  *
  * @param ds data store
  */
class RedisGeoMesaStats(ds: RedisDataStore, metadata: RedisBackedMetadata[Stat])
    extends MetadataBackedStats(ds, metadata) {

  @volatile
  private var run = true
  private var errors = 0

  // note: the default values ensure that these will always be Somes
  private val retries = RedisSystemProperties.TransactionRetries.toInt.get
  private val pause = RedisSystemProperties.TransactionPause.toDuration.get.toMillis
  private val rand = new SecureRandom()

  private val queue = new LinkedBlockingQueue[RedisStat]()
  private val es = java.util.concurrent.Executors.newScheduledThreadPool(2)

  es.submit(new Runnable() { override def run(): Unit = writeQueued() })

  override protected def write(typeName: String, stats: Seq[WritableStat]): Unit = {
    val queued = stats.forall { stat =>
      val keyBytes = metadata.encodeRow(typeName, stat.key)
      val serialized = metadata.serializer.serialize(typeName, stat.stat)
      queue.offer(RedisStat(typeName, stat.key, keyBytes, stat.stat, serialized, stat.merge, 0))
    }
    if (!queued) {
      logger.error(s"Could not queue stat for writing - queue size: ${queue.size()}")
    }
  }

  private def writeQueued(): Unit = {
    while (run) {
      try {
        val stat = queue.poll(1000L, TimeUnit.MILLISECONDS)
        if (stat != null) {
          val success = WithClose(ds.connection.getResource) { jedis =>
            // Use Lua script to atomically check, merge if needed, and write the stat
            val script = """
              local existing = redis.call('hget', KEYS[1], ARGV[1])
              local write
              if ARGV[3] == '1' then
                if existing then
                  return { existing, 0 } -- need to merge on client side
                else
                  write = ARGV[2]
                end
              else
                write = ARGV[2]
              end
              if write then
                redis.call('hset', KEYS[1], ARGV[1], write)
              end
              return { existing or false, write and 1 or 0 }
            """
            val merge = if (stat.merge) "1" else "0"
            val result = jedis.eval(
              script.getBytes(StandardCharsets.UTF_8),
              1,
              metadata.key,
              stat.keyBytes, stat.statBytes, merge.getBytes(StandardCharsets.UTF_8)
            )

            result match {
              case list: java.util.List[_] if !list.isEmpty =>
                val existing = Option(list.get(0)).map {
                  case bytes: Array[Byte] => bytes
                  case _ => null
                }
                val success = list.get(1) match {
                  case i: java.lang.Long => i.longValue() == 1L
                  case i: java.lang.Integer => i.intValue() == 1
                  case s: String => s.toInt == 1
                  case _ => false
                }

                if (success) {
                  true // write was successful
                } else if (existing.isDefined && stat.merge) {
                  // need to merge and retry
                  val merged = metadata.serializer.serialize(stat.typeName,
                    metadata.serializer.deserialize(stat.typeName, existing.get) + stat.stat)
                  val retryScript = """
                    redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
                    return 1
                  """
                  val retryResult = jedis.eval(
                    retryScript.getBytes(StandardCharsets.UTF_8),
                    1,
                    metadata.key,
                    stat.keyBytes, merged
                  )
                  retryResult match {
                    case i: java.lang.Long => i == 1L
                    case i: java.lang.Integer => i == 1
                    case s: String => s.toInt == 1
                    case _ => false
                  }
                } else {
                  false
                }

              case _ => false
            }
          }

          if (success) {
            // since we didn't write through the metadata instance, we have to invalidate the cache
            metadata.invalidateCache(stat.typeName, stat.key)
            errors = 0 // reset error count on success
          } else if (stat.attempt < retries) {
            // we had a conflict, so backoff and retry
            val delay = TransactionBackoffs.applyOrElse(stat.attempt, (_: Int) => TransactionBackoffs.last) * pause
            val runnable = new BackoffScheduler(stat.copy(attempt = stat.attempt + 1))
            es.schedule(runnable, delay + rand.nextInt(10), TimeUnit.MILLISECONDS)
          } else {
            throw new RuntimeException(
              s"Could not write stat for ${stat.typeName}:${stat.key} after ${stat.attempt + 1} attempts")
          }
        }
      } catch {
        case NonFatal(e) =>
          logger.error("Error in stat writing thread:", e)
          if (errors > 20) {
            logger.error("Terminating stat writing thread due to previous errors")
            run = false
            throw e
          } else {
            errors += 1
          }
      }
    }
  }

  override def close(): Unit = {
    run = false
    es.shutdown()
    super.close()
  }

  private class BackoffScheduler(stat: RedisStat) extends Runnable {
    override def run(): Unit = {
      if (!queue.offer(stat)) {
        logger.error(s"Could not queue stat for writing - queue size: ${queue.size()}")
      }
    }
  }
}

object RedisGeoMesaStats {

  def apply(ds: RedisDataStore): RedisGeoMesaStats = {
    val serializer = new StatsMetadataSerializer(ds)
    val metadata = new RedisBackedMetadata[Stat](ds.connection, s"${ds.config.catalog}_stats", serializer)
    new RedisGeoMesaStats(ds, metadata)
  }

  case class RedisStat(
      typeName: String,
      key: String,
      keyBytes: Array[Byte],
      stat: Stat,
      statBytes: Array[Byte],
      merge: Boolean,
      attempt: Int)
}
