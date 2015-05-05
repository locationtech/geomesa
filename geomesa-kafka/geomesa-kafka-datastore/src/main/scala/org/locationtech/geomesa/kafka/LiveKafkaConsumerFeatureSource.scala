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

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.google.common.eventbus.{EventBus, Subscribe}
import org.geotools.data.Query
import org.geotools.data.store.ContentEntry
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.index.SynchronizedQuadtree
import org.opengis.feature.simple.SimpleFeatureType

class LiveKafkaConsumerFeatureSource(entry: ContentEntry,
                                     schema: SimpleFeatureType,
                                     query: Query,
                                     topic: String,
                                     kf: KafkaConsumerFactory,
                                     expiry: Boolean,
                                     expirationPeriod: Long)
  extends KafkaConsumerFeatureSource(entry, schema, query, kf) {

  val eb = new EventBus(topic)
  eb.register(this)

  var qt = new SynchronizedQuadtree

  val cb = CacheBuilder.newBuilder()

  if (expiry) {
    cb.expireAfterWrite(expirationPeriod, TimeUnit.MILLISECONDS)
      .removalListener(
        new RemovalListener[String, FeatureHolder] {
          def onRemoval(removal: RemovalNotification[String, FeatureHolder]) = {
            qt.remove(removal.getValue.env, removal.getValue.sf)
          }
        }
      )
  }

  override val features: Cache[String, FeatureHolder] = cb.build()
  
  // create a producer that reads from kafka and sends to the event bus that the kcfs has subscribed to
  new KafkaFeatureConsumer(schema, topic, kf, eb)

  @Subscribe
  def processProtocolMessage(msg: GeoMessage): Unit = msg match {
    case update: CreateOrUpdate => processNewFeatures(update)
    case del: Delete            => removeFeature(del)
    case clr: Clear             => clear()
    case _     => throw new IllegalArgumentException("Unknown message: " + msg)
  }

  def processNewFeatures(update: CreateOrUpdate): Unit = {
    val sf = update.feature
    val id = update.id
    Option(features.getIfPresent(id)).foreach { old => qt.remove(old.env, old.sf) }
    val env = sf.geometry.getEnvelopeInternal
    qt.insert(env, sf)
    features.put(sf.getID, FeatureHolder(sf, env))
  }

  def removeFeature(toDelete: Delete): Unit = {
    val id = toDelete.id
    Option(features.getIfPresent(id)).foreach { old => qt.remove(old.env, old.sf) }
    features.invalidate(toDelete.id)
  }

  def clear(): Unit = {
    features.invalidateAll()
    qt = new SynchronizedQuadtree
  }
}

class KafkaFeatureConsumer(schema: SimpleFeatureType,
                           topic: String,
                           kf: KafkaConsumerFactory,
                           eventBus: EventBus) {

  private val msgDecoder = new KafkaGeoMessageDecoder(schema)

  private val stream = kf.messageStreams(topic, 1).head

  val es = Executors.newSingleThreadExecutor()
  es.submit(new Runnable {
    override def run(): Unit = {
      val iter = stream.iterator()
      while (iter.hasNext()) {
        val msg: GeoMessage = msgDecoder.decode(iter.next())
        eventBus.post(msg)
      }
    }
  })
}
