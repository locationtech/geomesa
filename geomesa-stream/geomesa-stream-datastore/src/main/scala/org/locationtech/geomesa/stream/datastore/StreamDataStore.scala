package org.locationtech.geomesa.stream.datastore

import java.awt.RenderingHints
import java.util.concurrent.{CopyOnWriteArrayList, Executors, TimeUnit}
import java.util.logging.Level
import java.{util => ju}

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.google.common.collect.Lists
import com.typesafe.config.ConfigFactory
import com.vividsolutions.jts.geom.{Envelope, Geometry}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data._
import org.geotools.data.collection.DelegateFeatureReader
import org.geotools.data.store._
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.collection.DelegateFeatureIterator
import org.geotools.filter.FidFilterImpl
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope}
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.stream.SimpleFeatureStreamSource
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.index.SynchronizedQuadtree
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.spatial.{BBOX, BinarySpatialOperator, Within}
import org.opengis.filter.{And, Filter, IncludeFilter, Or}

import scala.collection.JavaConversions._

case class FeatureHolder(sf: SimpleFeature, env: Envelope) {
  override def hashCode(): Int = sf.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: FeatureHolder => sf.equals(other.sf)
    case _ => false
  }
}

trait StreamListener {
  def onNext(sf: SimpleFeature): Unit
}

class StreamDataStore(source: SimpleFeatureStreamSource, timeout: Int) extends ContentDataStore {
  
  val sft = source.sft
  source.init()
  val qt = new SynchronizedQuadtree

  val cb =
    CacheBuilder
      .newBuilder()
      .expireAfterWrite(timeout, TimeUnit.SECONDS)
      .removalListener(
        new RemovalListener[String, FeatureHolder] {
          def onRemoval(removal: RemovalNotification[String, FeatureHolder]) = {
            qt.remove(removal.getValue.env, removal.getValue.sf)
          }
        }
      )

  val features: Cache[String, FeatureHolder] = cb.build()

  val listeners = new CopyOnWriteArrayList[StreamListener]()

  private val executor = Executors.newSingleThreadExecutor()
  executor.submit(
    new Runnable {
      override def run(): Unit = {
        while(true) {
          try {
            val sf = source.next
            if(sf != null) {
              val env = sf.geometry.getEnvelopeInternal
              qt.insert(env, sf)
              features.put(sf.getID, FeatureHolder(sf, env))
              listeners.foreach { l =>
                try {
                  l.onNext(sf)
                } catch {
                  case t: Throwable => getLogger.log(Level.WARNING, "Unable to notify listener", t)
                }
              }
            }
          } catch {
            case t: Throwable =>
            // swallow
          }
        }
      }
    }
  )

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource =
    new StreamFeatureStore(entry, null, features, qt, sft)

  def registerListener(listener: StreamListener): Unit = listeners.add(listener)

  override def createTypeNames(): ju.List[Name] = Lists.newArrayList(sft.getName)

  def close(): Unit = {
    try {
      executor.shutdown()
    } catch {
      case t: Throwable => // swallow
    }
  }
}

class StreamFeatureStore(entry: ContentEntry,
                         query: Query,
                         features: Cache[String, FeatureHolder],
                         qt: SynchronizedQuadtree,
                         sft: SimpleFeatureType)
  extends ContentFeatureStore(entry, query) {

  type FR = FeatureReader[SimpleFeatureType, SimpleFeature]
  type DFR = DelegateFeatureReader[SimpleFeatureType, SimpleFeature]
  type DFI = DelegateFeatureIterator[SimpleFeature]


  override def canFilter: Boolean = true

  override def getBoundsInternal(query: Query) =
    ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)

  override def buildFeatureType(): SimpleFeatureType = sft

  override def getCountInternal(query: Query): Int =
    getReaderInternal(query).getIterator.size

  override def getReaderInternal(query: Query): FR = getReaderForFilter(query.getFilter)

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

  def include(i: IncludeFilter) = new DFR(sft, new DFI(features.asMap().valuesIterator.map(_.sf)))

  def fid(ids: FidFilterImpl): FR = {
    val iter = ids.getIDs.flatMap(id => Option(features.getIfPresent(id.toString)).map(_.sf)).iterator
    new DFR(sft, new DFI(iter))
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
    new DFR(sft, new DFI(composed))
  }

  def within(w: Within): FR = {
    val (_, geomLit) = splitBinOp(w)
    val geom = geomLit.evaluate(null).asInstanceOf[Geometry]
    val res = qt.query(geom.getEnvelopeInternal)
    val filtered = res.asInstanceOf[java.util.List[SimpleFeature]].filter(sf => geom.contains(sf.point))
    val fiter = new DFI(filtered.iterator)
    new DFR(sft, fiter)
  }

  def bbox(b: BBOX): FR = {
    val bounds = JTS.toGeometry(b.getBounds)
    val res = qt.query(bounds.getEnvelopeInternal)
    val fiter = new DFI(res.asInstanceOf[java.util.List[SimpleFeature]].iterator)
    val filt = new FilteringFeatureIterator[SimpleFeature](fiter, b)
    new DFR(sft, filt)
  }

  def splitBinOp(binop: BinarySpatialOperator): (PropertyName, Literal) =
    binop.getExpression1 match {
      case pn: PropertyName => (pn, binop.getExpression2.asInstanceOf[Literal])
      case l: Literal       => (binop.getExpression2.asInstanceOf[PropertyName], l)
    }

  override def getWriterInternal(query: Query, flags: Int) = throw new IllegalArgumentException("Not allowed")


}

object StreamDataStoreParams {
  val STREAM_DATASTORE_CONFIG = new Param("geomesa.stream.datastore.config", classOf[String], "", true)
  val CACHE_TIMEOUT = new Param("geomesa.stream.datastore.cache.timeout", classOf[java.lang.Integer], "", true, 10)
}

class StreamDataStoreFactory extends DataStoreFactorySpi {

  import StreamDataStoreParams._

  override def createDataStore(params: ju.Map[String, java.io.Serializable]): DataStore = {
    val confString = STREAM_DATASTORE_CONFIG.lookUp(params).asInstanceOf[String]
    val timeout = Option(CACHE_TIMEOUT.lookUp(params)).map(_.asInstanceOf[Int]).getOrElse(10)
    val conf = ConfigFactory.parseString(confString)
    val source = SimpleFeatureStreamSource.buildSource(conf)
    new StreamDataStore(source, timeout)
  }

  override def createNewDataStore(params: ju.Map[String, java.io.Serializable]): DataStore = ???
  override def getDescription: String = "SimpleFeature Stream Source"
  override def getParametersInfo: Array[Param] = Array(STREAM_DATASTORE_CONFIG)
  override def getDisplayName: String = "SimpleFeature Stream Source"
  override def canProcess(params: ju.Map[String, java.io.Serializable]): Boolean =
    params.containsKey(STREAM_DATASTORE_CONFIG.key)

  override def isAvailable: Boolean = true
  override def getImplementationHints: ju.Map[RenderingHints.Key, _] = null
}