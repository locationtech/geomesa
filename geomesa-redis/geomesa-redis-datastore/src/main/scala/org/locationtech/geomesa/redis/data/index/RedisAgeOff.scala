/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data.index

import com.typesafe.scalalogging.StrictLogging
import org.geotools.api.data.Transaction
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.identity.Identifier
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.redis.data.index.RedisAgeOff.{AgeOffExecutor, AgeOffWriter}
import org.locationtech.geomesa.redis.data.util.RedisLocking
import org.locationtech.geomesa.redis.data.{CloseableJedisCommands, RedisDataStore, RedisSystemProperties}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.WithClose
import redis.clients.jedis.{Jedis, UnifiedJedis}
import redis.clients.jedis.params.ZAddParams
import redis.clients.jedis.util.Pool

import java.io.{Closeable, Flushable}
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
  * Tracks and deletes features based on a time-to-live.
  *
  * TTLs are stored in a separate sorted set for each feature type, with the score of the set being the
  * system time in millis when the feature should be expired. We do a two-step process to expire features:
  *
  * 1. Scan and remove the TTL set by score range, which will return all the feature IDs for expired features
  * 2. Remove the features from the index tables based on the feature ID
  *
  * @param ds data store
  */
class RedisAgeOff(ds: RedisDataStore) extends Closeable {

  import RedisAgeOff.key
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  private val executor = RedisSystemProperties.AgeOffInterval.toDuration.collect {
    case i if i.isFinite => new AgeOffExecutor(ds, i)
  }

  /**
    * Start the age-off for a feature type
    *
    * @param sft simple feature type
    */
  def add(sft: SimpleFeatureType): Unit = {
    if (sft.isFeatureExpirationEnabled) {
      executor.foreach(_.schedule(sft.getTypeName))
    }
  }

  /**
    * Update the age-off for a modified feature type
    *
    * @param sft modified simple feature type
    * @param previous previous simple feature type
    */
  def update(sft: SimpleFeatureType, previous: SimpleFeatureType): Unit = {
    if (previous.isFeatureExpirationEnabled) {
      if (!sft.isFeatureExpirationEnabled) {
        remove(previous)
      } else if (sft.getTypeName != previous.getTypeName) {
        executor.foreach(_.cancel(previous.getTypeName))
        // rename the ttl table to match the new type name
        WithClose(ds.connection.getResource)(_.renamenx(key(ds, previous.getTypeName), key(ds, sft.getTypeName)))
        executor.foreach(_.schedule(sft.getTypeName))
      }
    } else if (sft.isFeatureExpirationEnabled) {
      executor.foreach(_.schedule(sft.getTypeName))
    }
  }

  /**
    * Remove the age-off for a feature type
    *
    * @param sft simple feature type
    */
  def remove(sft: SimpleFeatureType): Unit = {
    if (sft.isFeatureExpirationEnabled) {
      executor.foreach(_.cancel(sft.getTypeName))
      WithClose(ds.connection.getResource)(_.del(key(ds, sft.getTypeName)))
    }
  }

  /**
    * Gets a writer to track ttl for features
    *
    * @param sft simple feature type
    * @return
    */
  def writer(sft: SimpleFeatureType): Option[AgeOffWriter] = {
    if (sft.isFeatureExpirationEnabled) {
      Some(new AgeOffWriter(ds.connection, key(ds, sft.getTypeName)))
    } else {
      None
    }
  }

  override def close(): Unit = executor.foreach(_.close())
}

object RedisAgeOff extends StrictLogging {

  import redis.clients.jedis.resps.{Tuple => JedisTuple}

  val TtlUserDataKey = "t" // the user data is cleared, so no chance of key collision - save space with a short key

  val AgeOffLockTimeout: SystemProperty = SystemProperty("geomesa.redis.age.off.lock.timeout", "1 second")

  /**
    * Initialize the age-off for the data store
    *
    * @param ds data store
    */
  def init(ds: RedisDataStore): Unit = ds.getTypeNames.foreach(n => ds.aging.add(ds.getSchema(n)))

  /**
    * Key of the Redis SortedSet containing the ttl for the given feature type
    *
    * @param ds data store
    * @param typeName simple feature type name
    * @return
    */
  private def key(ds: RedisDataStore, typeName: String): Array[Byte] =
    s"${ds.config.catalog}_${typeName}_ttl".getBytes(StandardCharsets.UTF_8)

  /**
    * Writes time-to-lives for new features
    *
    * @param connection jedis connection
    * @param table ttl table for the feature type
    */
  class AgeOffWriter(connection: Pool[_ <: CloseableJedisCommands], table: Array[Byte]) extends Closeable with Flushable {

    private val writes = new java.util.HashMap[Array[Byte], java.lang.Double]
    private val deletes = ArrayBuffer.empty[Array[Byte]]

    // note: the regular feature ids have the length pre-pended as a 2-byte short, so use the raw id instead
    def write(feature: RedisWritableFeature): Unit = writes.put(feature.rawId, feature.ttl)
    def delete(feature: RedisWritableFeature): Unit = deletes += feature.rawId

    override def flush(): Unit = {
      if (!writes.isEmpty || deletes.nonEmpty) {
        try {
          WithClose(connection.getResource) { jedis =>
            if (deletes.nonEmpty) {
              jedis.zrem(table, deletes.toSeq: _*)
            }
            if (!writes.isEmpty) {
              jedis.zadd(table, writes)
            }
          }
        } finally {
          writes.clear()
          deletes.clear()
        }
      }
    }

    override def close(): Unit = flush()
  }

  /**
    * Class for tracking scheduled expiration tasks
    *
    * @param ds data store
    * @param frequency run frequency
    */
  private class AgeOffExecutor(ds: RedisDataStore, frequency: Duration) extends Closeable {

    // expiration tasks, keyed by feature type name
    // note: synchronize access to ensure thread safety
    private val tasks = scala.collection.mutable.Map.empty[String, ScheduledFuture[_]]
    private val rate = frequency.toMillis
    private val es = Executors.newScheduledThreadPool(3)

    /**
      * Schedule the removal of features for the feature type
      *
      * @param typeName simple feature type name
      */
    def schedule(typeName: String): Unit = {
      // schedule with a short initial delay to quickly remove any features that may
      // have expired since the last time there was an active datastore running
      val future = es.scheduleAtFixedRate(new AgeOffRunner(ds, typeName), 5000L, rate, TimeUnit.MILLISECONDS)
      synchronized {
        tasks.put(typeName, future).foreach(_.cancel(false))
      }
    }

    /**
      * Cancel any scheduled tasks for the feature type
      *
      * @param typeName simple feature type name
      */
    def cancel(typeName: String): Unit = {
      synchronized {
        tasks.remove(typeName).foreach(_.cancel(false))
      }
    }

    override def close(): Unit = es.shutdown()
  }

  /**
    * Runnable class to check and remove expired features
    *
    * @param ds data store
    * @param typeName simple feature type name to check
    */
  private class AgeOffRunner(ds: RedisDataStore, typeName: String) extends Runnable with RedisLocking {

    import scala.collection.JavaConverters._

    private val table = key(ds, typeName)
    private val lockPath = s"/org.locationtech.geomesa.redis.${ds.config.catalog}.ttl.$typeName"

    private val timeout = AgeOffLockTimeout.toDuration.map(_.toMillis).getOrElse {
      // note: should always be valid due to the default
      throw new IllegalStateException("Invalid age-off lock timeout")
    }

    override def connection: Pool[_ <: CloseableJedisCommands] = ds.connection

    override def run(): Unit = {
      val timestamp = System.currentTimeMillis()
      logger.debug(s"Age-off for schema '$typeName' starting with timestamp $timestamp")

      val ids = removeExpiredIds(timestamp)
      if (!ids.isEmpty) {
        removeExpiredFeatures(ids, timestamp)
      }
    }

    /**
      * Queries and removes expired ids and ttls
      *
      * @param timestamp expiration time
      * @return
      */
    private def removeExpiredIds(timestamp: Long): java.util.List[JedisTuple] = {
      def exec: java.util.List[JedisTuple] = {
        WithClose(ds.connection.getResource) { jedis =>
          // Use Lua script to atomically get and remove expired entries
          val script = """
            local scores = redis.call('zrangebyscore', KEYS[1], 0, ARGV[1], 'WITHSCORES')
            if #scores > 0 then
              redis.call('zremrangebyscore', KEYS[1], 0, ARGV[1])
            end
            return scores
          """
          val result = jedis.eval(script.getBytes(StandardCharsets.UTF_8), 1, table, timestamp.toString.getBytes(StandardCharsets.UTF_8))
          // Convert the result to a List[JedisTuple]
          result match {
            case list: java.util.List[_] if list.size() > 0 =>
              val tuples = new java.util.ArrayList[JedisTuple](list.size() / 2)
              var i = 0
              while (i < list.size()) {
                val member = list.get(i).asInstanceOf[Array[Byte]]
                val score = list.get(i + 1) match {
                  case s: String => s.toDouble
                  case b: Array[Byte] => new String(b, StandardCharsets.UTF_8).toDouble
                  case _ => throw new IllegalStateException(s"Unexpected score type: ${list.get(i + 1).getClass}")
                }
                tuples.add(new JedisTuple(member, score))
                i += 2
              }
              tuples
            case _ => Collections.emptyList[JedisTuple]()
          }
        }
      }

      def noLock: java.util.List[JedisTuple] = {
        logger.debug(s"Could not acquire distributed lock for schema '$typeName' after ${timeout}ms")
        Collections.emptyList[JedisTuple]()
      }

      // acquire a lock so that we're not repeating work in multiple data store instances
      val ids = try { withLock(lockPath, timeout, exec, noLock) } catch {
        case NonFatal(e) =>
          logger.error("Error executing ttl script:", e)
          Collections.emptyList[JedisTuple]()
      }

      logger.debug(s"Age-off for schema '$typeName' found ${ids.size} features for expiration")

      ids
    }

    /**
      * Removes expired features from the data store
      *
      * @param ids expired ids and ttls
      * @param timestamp expiration time
      */
    private def removeExpiredFeatures(ids: java.util.List[JedisTuple], timestamp: Long): Unit = {
      try {
        val fids = new java.util.HashSet[Identifier](ids.size)
        ids.asScala.foreach(id => fids.add(FilterHelper.ff.featureId(id.getElement)))

        logger.trace(s"Age-off for schema '$typeName' found the following features: $fids")
        val trace = logger.underlying.isTraceEnabled
        lazy val results = ListBuffer.empty[String]

        var i = 0
        WithClose(ds.getFeatureWriter(typeName, FilterHelper.ff.id(fids), Transaction.AUTO_COMMIT)) { writer =>
          while (writer.hasNext) {
            val sf = writer.next()
            // verify that the feature hasn't been updated since we fetched the expired ids
            // note: if the feature is updated between our call to `next` and `remove`, the
            // member keys won't match and our deletion will not affect anything, as desired
            val ttl = sf.getUserData.get(TtlUserDataKey)
            if (ttl != null && ttl.asInstanceOf[Long] <= timestamp) {
              writer.remove()
              i += 1
              if (trace) {
                results += sf.getID
              }
            } else {
              logger.debug(s"Age-off for schema '$typeName' ignoring updated feature: ${sf.getID}")
            }
          }
        }
        logger.debug(s"Age-off for schema '$typeName' removed $i features")
        logger.trace(s"Age-off for schema '$typeName' removed the following features: [${results.mkString(", ")}]")
      } catch {
        case NonFatal(e) =>
          logger.error(s"Error executing age-off for schema '$typeName':", e)
          // try to re-insert the keys, otherwise they might never expire
          reinsertFailedIds(ids)
      }
    }

    /**
      * Reinserts ids for features that were not deleted from the data store
      *
      * @param ids expired ids and ttls
      */
    private def reinsertFailedIds(ids: java.util.List[JedisTuple]): Unit = {
      try {
        logger.debug(s"Age-off for schema '$typeName' re-inserting ttls")
        val reinserts = new java.util.HashMap[Array[Byte], java.lang.Double](ids.size)
        ids.asScala.foreach(id => reinserts.put(id.getBinaryElement, id.getScore))
        // use nx so that if a feature update has come through we don't overwrite the new ttl
        WithClose(ds.connection.getResource)(_.zadd(table, reinserts, new ZAddParams().nx()))
      } catch {
        case NonFatal(e) =>
          logger.error(s"Error re-inserting ttls for schema '$typeName'. The following features " +
              s"may never expire: [${ids.asScala.map(_.getElement).mkString(", ")}]", e)
      }
    }
  }
}
