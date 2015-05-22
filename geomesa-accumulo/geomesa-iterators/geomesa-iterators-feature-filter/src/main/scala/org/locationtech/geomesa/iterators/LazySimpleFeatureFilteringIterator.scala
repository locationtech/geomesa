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
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.features.nio.{AttributeAccessor, LazySimpleFeature}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.reflect.ClassTag

trait LazyFilterTransformIterator extends SortedKeyValueIterator[Key, Value] with Logging {

  import LazyFilterTransformIterator._

  var sft: SimpleFeatureType = null
  var src: SortedKeyValueIterator[Key, Value] = null
  var filter: Filter = null
  var transform: String = null
  var transformSchema: SimpleFeatureType = null
  var topValue: Value = new Value()

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    LazyFilterTransformIterator.initClassLoader(logger)
    src = source
    sft = SimpleFeatureTypes.createType("test", options.get(SFT_OPT))
    filter = Option(options.get(CQL_OPT)).map(FastFilterFactory.toFilter).orNull
    transform = Option(options.get(TRANSFORM_DEFINITIONS_OPT)).orNull
    transformSchema = Option(options.get(TRANSFORM_SCHEMA_OPT)).map(SimpleFeatureTypes.createType("", _)).orNull
  }

  def sf: SimpleFeature
  def initReusableFeature(buf: Array[Byte]): Unit
  def setTransform(): Unit

  override def next(): Unit = {
    src.next()
    findTop()
  }

  def findTop(): Unit = {
    var found = false
    while (!found && src.hasTop) {
      initReusableFeature(src.getTopValue.get())
      if (filter == null || filter.evaluate(sf)) {
        found = true
      } else {
        src.next()
      }
    }
  }

  override def getTopKey: Key = src.getTopKey
  override def getTopValue: Value =
    if (transform == null) {
      src.getTopValue
    } else {
      setTransform()
      topValue
    }
  override def hasTop: Boolean = src.hasTop

  override def seek(range: Range, columnFamilies: jCollection[ByteSequence], inclusive: Boolean): Unit = {
    src.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

class KryoLazyFilterTransformIterator extends LazyFilterTransformIterator {

  private var kryo: KryoFeatureSerializer = null
  private var reusablesf: KryoBufferSimpleFeature = null

  override def sf: SimpleFeature = reusablesf

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    super.init(source, options, env)
    kryo = new KryoFeatureSerializer(sft)
    reusablesf = kryo.getReusableFeature
    if (transform != null && transformSchema != null) {
      reusablesf.setTransforms(transform, transformSchema)
    }
  }

  override def initReusableFeature(buf: Array[Byte]): Unit = reusablesf.setBuffer(buf)

  override def setTransform(): Unit = topValue.set(reusablesf.transform())
}

class NIOLazyFilterTransformIterator extends LazyFilterTransformIterator {

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

  override def setTransform(): Unit = topValue.set(src.getTopValue.get()) // TODO
}

object LazyFilterTransformIterator {

  val SFT_OPT                   = "sft"
  val CQL_OPT                   = "cql"
  val TRANSFORM_SCHEMA_OPT      = "tsft"
  val TRANSFORM_DEFINITIONS_OPT = "tdefs"

  def configure[T <: LazyFilterTransformIterator](sft: SimpleFeatureType,
                                                  filter: Option[Filter],
                                                  transform: Option[(String, SimpleFeatureType)],
                                                  priority: Int)(implicit ct: ClassTag[T]) = {
    assert(filter.isDefined || transform.isDefined, "No options configured")
    val is = new IteratorSetting(priority, "featurefilter", ct.runtimeClass.getCanonicalName)
    is.addOption(SFT_OPT, SimpleFeatureTypes.encodeType(sft))
    filter.foreach(f => is.addOption(CQL_OPT, ECQL.toCQL(f)))
    transform.foreach { case (tdef, tsft) =>
      is.addOption(TRANSFORM_DEFINITIONS_OPT, tdef)
      is.addOption(TRANSFORM_SCHEMA_OPT, SimpleFeatureTypes.encodeType(tsft))
    }
    is
  }

  private var initialized = false

  def initClassLoader(log: Logger) = synchronized {
    if (!initialized) {
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
        initialized = true
      }
    }
  }
}