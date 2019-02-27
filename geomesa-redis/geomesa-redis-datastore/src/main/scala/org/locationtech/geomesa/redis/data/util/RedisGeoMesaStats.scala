/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data
package util

import java.security.SecureRandom
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import org.locationtech.geomesa.index.stats.MetadataBackedStats
import org.locationtech.geomesa.index.stats.MetadataBackedStats.{StatsMetadataSerializer, WritableStat}
import org.locationtech.geomesa.redis.data.util.RedisGeoMesaStats.RedisStat
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.Stat

import scala.util.control.NonFatal

/**
  * Redis stats implementation
  *
  * @param ds data store
  */
class RedisGeoMesaStats(ds: RedisDataStore, metadata: RedisBackedMetadata[Stat])
    extends MetadataBackedStats(ds, metadata, ds.config.generateStats) {

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

  private def writeQueued(): Unit = {
    while (run) {
      try {
        val stat = queue.poll(1000L, TimeUnit.MILLISECONDS)
        if (stat != null) {
          // use a redis watch to ensure that we aren't overwriting some other change
          val success = WithClose(ds.connection.getResource) { jedis =>
            jedis.watch(metadata.key)
            val write = if (stat.merge) {
              val existing = jedis.hget(metadata.key, stat.keyBytes)
              if (existing == null) { stat.statBytes } else {
                metadata.serializer.serialize(stat.typeName,
                  metadata.serializer.deserialize(stat.typeName, existing) + stat.stat)
              }
            } else {
              stat.statBytes
            }
            // with a watch, we have to use a transaction to ensure the value hasn't changed
            val tx = jedis.multi()
            tx.hset(metadata.key, stat.keyBytes, write)
            tx.exec() != null // null means invalid
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
