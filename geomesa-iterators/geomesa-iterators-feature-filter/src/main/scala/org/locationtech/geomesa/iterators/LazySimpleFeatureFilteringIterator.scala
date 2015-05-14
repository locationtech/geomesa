package org.locationtech.geomesa.iterators

import java.nio.ByteBuffer
import java.util

import com.typesafe.scalalogging.slf4j.{Logger, Logging}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Range, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.vfs2.impl.VFSClassLoader
import org.geotools.factory.GeoTools
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.feature.nio.{AttributeAccessor, LazySimpleFeature}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class LazySimpleFeatureFilteringIterator extends SortedKeyValueIterator[Key, Value] with Logging {

  private var sft: SimpleFeatureType = null
  private var accessors: IndexedSeq[AttributeAccessor[_ <: AnyRef]] = null
  private var src: SortedKeyValueIterator[Key, Value] = null
  private var reusableSimpleFeature: LazySimpleFeature = null
  private var filter: Filter = null

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    LazySimpleFeatureFilteringIterator.initClassLoader(logger)
    src = source
    sft = SimpleFeatureTypes.createType("test", options.get("sft"))
    accessors = AttributeAccessor.buildSimpleFeatureTypeAttributeAccessors(sft)
    reusableSimpleFeature = new LazySimpleFeature("", sft, accessors, null)
    filter = ECQL.toFilter(options.get("cql"))
  }

  override def next(): Unit = {
    src.next()
    findTop()
  }

  def findTop(): Unit = {
    var found = false
    while(src.hasTop && !found) {
      reusableSimpleFeature.setBuf(ByteBuffer.wrap(src.getTopValue.get()))
      if(!filter.evaluate(reusableSimpleFeature)) src.next()
      else found = true
    }
  }

  override def getTopKey: Key = src.getTopKey
  override def getTopValue: Value = src.getTopValue
  override def hasTop: Boolean = src.hasTop

  override def seek(range: Range,
                    columnFamilies: util.Collection[ByteSequence],
                    inclusive: Boolean): Unit = {
    src.seek(range, columnFamilies, inclusive)
    findTop()
  }


}

object LazySimpleFeatureFilteringIterator {
  def configure(sft: SimpleFeatureType, filter: Filter) = {
    val is = new IteratorSetting(5, "featurefilter", classOf[LazySimpleFeatureFilteringIterator].getCanonicalName)
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