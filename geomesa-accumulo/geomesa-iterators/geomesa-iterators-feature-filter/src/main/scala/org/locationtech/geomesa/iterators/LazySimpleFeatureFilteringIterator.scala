package org.locationtech.geomesa.iterators

import java.nio.ByteBuffer
import java.util
import java.util.{Collection => jCollection, Map => jMap}

import com.typesafe.scalalogging.slf4j.{Logger, Logging}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.vfs2.impl.VFSClassLoader
import org.geotools.factory.GeoTools
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.feature.nio.{AttributeAccessor, LazySimpleFeature}
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.reflect.ClassTag

trait LazySFFilterIter extends SortedKeyValueIterator[Key, Value] with Logging {

  var sft: SimpleFeatureType = null
  var src: SortedKeyValueIterator[Key, Value] = null
  var filter: Filter = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    LazySimpleFeatureFilteringIterator.initClassLoader(logger)
    src = source
    sft = SimpleFeatureTypes.createType("test", options.get("sft"))
    filter = FastFilterFactory.toFilter(options.get("cql"))
  }

  def sf: SimpleFeature
  def initReusableFeature(buf: Array[Byte]): Unit

  override def next(): Unit = {
    src.next()
    findTop()
  }

  def findTop(): Unit = {
    var found = false
    while (!found && src.hasTop) {
      initReusableFeature(src.getTopValue.get())
      if (filter.evaluate(sf)) {
        found = true
      } else {
        src.next()
      }
    }
  }

  override def getTopKey: Key = src.getTopKey
  override def getTopValue: Value = src.getTopValue
  override def hasTop: Boolean = src.hasTop

  override def seek(range: Range, columnFamilies: jCollection[ByteSequence], inclusive: Boolean): Unit = {
    src.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

class LazyKryoFeatureFilteringIterator extends LazySFFilterIter {

  private var kryo: KryoFeatureSerializer = null
  private var reusablesf: KryoBufferSimpleFeature = null

  override def sf: SimpleFeature = reusablesf

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(source, options, env)
    kryo = new KryoFeatureSerializer(sft)
    reusablesf = kryo.getReusableFeature
  }

  override def initReusableFeature(buf: Array[Byte]): Unit = reusablesf.setBuffer(buf)
}

class LazyNIOFeatureFilteringIterator extends LazySFFilterIter {

  private var accessors: IndexedSeq[AttributeAccessor[_ <: AnyRef]] = null
  private var reusablesf: LazySimpleFeature = null

  override def sf: SimpleFeature = reusablesf

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(source, options, env)
    accessors = AttributeAccessor.buildSimpleFeatureTypeAttributeAccessors(sft)
    reusablesf = new LazySimpleFeature("", sft, accessors, null)
  }

  override def initReusableFeature(buf: Array[Byte]): Unit = reusablesf.setBuf(ByteBuffer.wrap(buf))
}

object LazySimpleFeatureFilteringIterator {

  def configure[T <: LazySFFilterIter](sft: SimpleFeatureType, filter: Filter)(implicit ct: ClassTag[T]) = {
    val is = new IteratorSetting(5, "featurefilter", ct.runtimeClass.getCanonicalName)
    is.addOption("sft", SimpleFeatureTypes.encodeType(sft))
    is.addOption("cql", ECQL.toCQL(filter))
    is
  }

  val initialized = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }

  def initClassLoader(log: Logger) =
    if(!initialized.get()) {
      try {
        log.trace("Initializing classLoader")
        // locate the geomesa-distributed-runtime jar
        val cl = this.getClass.getClassLoader
        cl match {
          case vfsCl: VFSClassLoader =>
            var url = vfsCl.getFileObjects.map(_.getURL).filter {
              _.toString.contains("geomesa-distributed-runtime")
            }.head
            if (log != null) log.debug(s"Found geomesa-distributed-runtime at $url")
            var u = java.net.URLClassLoader.newInstance(Array(url), vfsCl)
            GeoTools.addClassLoader(u)

            url = vfsCl.getFileObjects.map(_.getURL).filter {
              _.toString.contains("geomesa-feature")
            }.head
            if (log != null) log.debug(s"Found geomesa-feature at $url")
            u = java.net.URLClassLoader.newInstance(Array(url), vfsCl)
            GeoTools.addClassLoader(u)

          case _ =>
        }
      } catch {
        case t: Throwable =>
          if(log != null) log.error("Failed to initialize GeoTools' ClassLoader ", t)
      } finally {
        initialized.set(true)
      }
    }

}