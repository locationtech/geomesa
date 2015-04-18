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

import java.nio.charset.StandardCharsets
import java.util
import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.google.common.eventbus.{EventBus, Subscribe}
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import kafka.producer.KeyedMessage
import kafka.serializer.DefaultDecoder
import org.geotools.data.collection.DelegateFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FilteringFeatureReader, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.collection.DelegateFeatureIterator
import org.geotools.filter.FidFilterImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.feature.AvroFeatureDecoder
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.ContentFeatureSourceReTypingSupport
import org.locationtech.geomesa.utils.index.SynchronizedQuadtree
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.identity.FeatureId
import org.opengis.filter.spatial.{BBOX, BinarySpatialOperator, Within}
import org.opengis.filter.{And, Filter, IncludeFilter, Or}

import scala.collection.JavaConversions._

class KafkaConsumerFeatureSource(entry: ContentEntry,
                                 schema: SimpleFeatureType,
                                 eb: EventBus,
                                 query: Query,
                                 expiry: Boolean,
                                 expirationPeriod: Long)
  extends ContentFeatureStore(entry, query)
  with ContentFeatureSourceReTypingSupport {

  var qt = new SynchronizedQuadtree

  case class FeatureHolder(sf: SimpleFeature, env: Envelope) {
    override def hashCode(): Int = sf.hashCode()

    override def equals(obj: scala.Any): Boolean = obj match {
      case other: FeatureHolder => sf.equals(other.sf)
      case _ => false
    }
  }

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

  val features: Cache[String, FeatureHolder] = cb.build()

  eb.register(this)

  @Subscribe
  def processProtocolMessage(msg: KafkaGeoMessage): Unit = msg match {
    case update: CreateOrUpdate => processNewFeatures(update)
    case del: Delete            => removeFeature(del)
    case Clear                  => clear()
    case _     => throw new IllegalArgumentException("Should never happen")
  }

  def processNewFeatures(update: CreateOrUpdate): Unit = {
    val sf = update.f
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

  override def getBoundsInternal(query: Query) =
    ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)

  override def buildFeatureType(): SimpleFeatureType = schema

  override def getCountInternal(query: Query): Int =
    getReaderInternal(query).getIterator.size

  override def getReaderInternal(query: Query): FR = addSupport(query, getReaderForFilter(query.getFilter))

  def getReaderForFilter(f: Filter): FR =
    f match {
      case o: Or             => or(o)
      case i: IncludeFilter  => include(i)
      case w: Within         => within(w)
      case b: BBOX           => bbox(b)
      case a: And            => and(a)
      case id: FidFilterImpl => fid(id)
      case _                 =>
        new FilteringFeatureReader[SimpleFeatureType, SimpleFeature](include(Filter.INCLUDE), f)
    }

  type DFR = DelegateFeatureReader[SimpleFeatureType, SimpleFeature]
  type DFI = DelegateFeatureIterator[SimpleFeature]

  def include(i: IncludeFilter) = new DFR(schema, new DFI(features.asMap().valuesIterator.map(_.sf)))

  def fid(ids: FidFilterImpl): FR = {
    val iter = ids.getIDs.flatMap(id => Option(features.getIfPresent(id.toString)).map(_.sf)).iterator
    new DFR(schema, new DFI(iter))
  }

  private val ff = CommonFactoryFinder.getFilterFactory2
  def and(a: And): FR = {
    // assume just one spatialFilter for now, i.e. 'bbox() && attribute equals ??'
    val (spatialFilter, others) = a.getChildren.partition(_.isInstanceOf[BinarySpatialOperator])
    val restFilter = ff.and(others)
    val filterIter = spatialFilter.headOption.map(getReaderForFilter).getOrElse(include(Filter.INCLUDE))
    new FilteringFeatureReader[SimpleFeatureType, SimpleFeature](filterIter, restFilter)
  }

  def or(o: Or): FR = {
    val readers = o.getChildren.map(getReaderForFilter).map(_.getIterator)
    val composed = readers.foldLeft(Iterator[SimpleFeature]())(_ ++ _)
    new DFR(schema, new DFI(composed))
  }

  def within(w: Within): FR = {
    val (_, geomLit) = splitBinOp(w)
    val geom = geomLit.evaluate(null).asInstanceOf[Geometry]
    val res = qt.query(geom.getEnvelopeInternal)
    val filtered = res.asInstanceOf[java.util.List[SimpleFeature]].filter(sf => geom.contains(sf.point))
    val fiter = new DFI(filtered.iterator)
    new DFR(schema, fiter)
  }

  def bbox(b: BBOX): FR = {
    val bounds = JTS.toGeometry(b.getBounds)
    val res = qt.query(bounds.getEnvelopeInternal)
    val fiter = new DFI(res.asInstanceOf[java.util.List[SimpleFeature]].iterator)
    new DFR(schema, fiter)
  }

  def splitBinOp(binop: BinarySpatialOperator): (PropertyName, Literal) =
    binop.getExpression1 match {
      case pn: PropertyName => (pn, binop.getExpression2.asInstanceOf[Literal])
      case l: Literal       => (binop.getExpression2.asInstanceOf[PropertyName], l)
    }

  private var id = 1L
  def getNextId: FeatureId = {
    val ret = id
    id += 1
    new FeatureIdImpl(ret.toString)
  }

  override def getWriterInternal(query: Query, flags: Int) = throw new IllegalArgumentException("Not allowed")
}

sealed trait KafkaGeoMessage
case class CreateOrUpdate(id: String, f: SimpleFeature)  extends KafkaGeoMessage
case class Delete(id: String) extends KafkaGeoMessage
case object Clear extends KafkaGeoMessage {
  type MSG = KeyedMessage[Array[Byte], Array[Byte]]
  val EMPTY = Array.empty[Byte]
  def toMsg(topic: String) = new MSG(topic, KafkaProducerFeatureStore.CLEAR_KEY, EMPTY)
}

trait FeatureProducer {
  def eventBus: EventBus
  def produceFeatures(f: SimpleFeature): Unit = eventBus.post(CreateOrUpdate(f.getID, f))
  def deleteFeature(id: String): Unit = eventBus.post(Delete(id))
  def deleteFeatures(ids: Seq[String]): Unit = ids.foreach(deleteFeature)
  def clear(): Unit = eventBus.post(Clear)
}

class KafkaFeatureConsumer(topic: String,
                           zookeepers: String,
                           groupId: String,
                           featureDecoder: AvroFeatureDecoder,
                           override val eventBus: EventBus) extends FeatureProducer {

  private val client = Consumer.create(new ConsumerConfig(buildClientProps))
  private val whiteList = new Whitelist(topic)
  private val decoder: DefaultDecoder = new DefaultDecoder(null)
  private val stream = client.createMessageStreamsByFilter(whiteList, 1, decoder, decoder).head

  val es = Executors.newSingleThreadExecutor()
  es.submit(new Runnable {
    override def run(): Unit = {
      val iter = stream.iterator()
      while (iter.hasNext) {
        val msg = iter.next()
        if(msg.key() != null) {
          if(util.Arrays.equals(msg.key(), KafkaProducerFeatureStore.DELETE_KEY)) {
            val id = new String(msg.message(), StandardCharsets.UTF_8)
            deleteFeature(id)
          } else if(util.Arrays.equals(msg.key(), KafkaProducerFeatureStore.CLEAR_KEY)) {
            clear()
          } else {
            // only other key is the SCHEMA_KEY so ingore
          }
        } else {
          val f = featureDecoder.decode(msg.message())
          produceFeatures(f)
        }
      }
    }
  })

  private def buildClientProps = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeepers)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "2000")
    props.put("zookeeper.sync.time.ms", "1000")
    props.put("auto.commit.interval.ms", "1000")
    props
  }
}
