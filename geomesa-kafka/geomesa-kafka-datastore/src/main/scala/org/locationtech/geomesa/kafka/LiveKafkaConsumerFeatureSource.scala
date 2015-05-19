/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.kafka

import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.base.Ticker
import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.google.common.eventbus.{EventBus, Subscribe}
import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.Query
import org.geotools.data.store.ContentEntry
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.FR
import org.locationtech.geomesa.utils.index.SynchronizedQuadtree
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConverters._
import scala.collection.mutable

class LiveKafkaConsumerFeatureSource(entry: ContentEntry,
                                     sft: SimpleFeatureType,
                                     topic: String,
                                     kf: KafkaConsumerFactory,
                                     expirationPeriod: Option[Long] = None,
                                     query: Query = null)
                                    (implicit ticker: Ticker = Ticker.systemTicker())
  extends KafkaConsumerFeatureSource(entry, sft, query) {

  private[kafka] val featureCache = new LiveFeatureCache(sft, expirationPeriod)

  val eb = new EventBus(topic)
  eb.register(this)

  // create a consumer that reads from kafka and sends to the event bus
  new KafkaFeatureConsumer(sft, topic, kf, eb)

  @Subscribe
  def processProtocolMessage(msg: GeoMessage): Unit = msg match {
    case update: CreateOrUpdate => featureCache.createOrUpdateFeature(update)
    case del: Delete            => featureCache.removeFeature(del)
    case clr: Clear             => featureCache.clear()
    case _     => throw new IllegalArgumentException("Unknown message: " + msg)
  }

  override def getReaderForFilter(f: Filter): FR = featureCache.getReaderForFilter(f)
}

/** @param schema the [[SimpleFeatureType]]
  * @param expirationPeriod the number of milliseconds after write to expire a feature or ``None`` to not
  *                         expire
  * @param ticker used to determine elapsed time for expiring entries
  */
class LiveFeatureCache(override val schema: SimpleFeatureType,
                       expirationPeriod: Option[Long])(implicit ticker: Ticker)
  extends KafkaConsumerFeatureCache {

  var qt = new SynchronizedQuadtree

  val cache: Cache[String, FeatureHolder] = {

    val cb = CacheBuilder.newBuilder().ticker(ticker)

    expirationPeriod.map { ep =>
      cb.expireAfterWrite(ep, TimeUnit.MILLISECONDS)
        .removalListener(
          new RemovalListener[String, FeatureHolder] {
            def onRemoval(removal: RemovalNotification[String, FeatureHolder]) = {
              qt.remove(removal.getValue.env, removal.getValue.sf)
            }
          }
        )
    }

    cb.build()
  }

  override val features: mutable.Map[String, FeatureHolder] = cache.asMap().asScala

  def createOrUpdateFeature(update: CreateOrUpdate): Unit = {
    val sf = update.feature
    val id = update.id
    Option(cache.getIfPresent(id)).foreach { old => qt.remove(old.env, old.sf) }
    val env = sf.geometry.getEnvelopeInternal
    qt.insert(env, sf)
    cache.put(sf.getID, FeatureHolder(sf, env))
  }

  def removeFeature(toDelete: Delete): Unit = {
    val id = toDelete.id
    Option(cache.getIfPresent(id)).foreach { old => qt.remove(old.env, old.sf) }
    cache.invalidate(toDelete.id)
  }

  def clear(): Unit = {
    cache.invalidateAll()
    qt = new SynchronizedQuadtree
  }
}

class KafkaFeatureConsumer(sft: SimpleFeatureType,
                           topic: String,
                           kf: KafkaConsumerFactory,
                           eventBus: EventBus) extends Logging {

  private val msgDecoder = new KafkaGeoMessageDecoder(sft)

  private val stream = kf.messageStreams(topic, 1).head

  val es = Executors.newSingleThreadExecutor()
  es.submit(new Runnable {
    override def run(): Unit = {
      val iter = stream.iterator()
      while (iter.hasNext()) {
        val msg: GeoMessage = msgDecoder.decode(iter.next())
        logger.debug("consumed message: {}", msg)
        eventBus.post(msg)
      }
    }
  })
}
