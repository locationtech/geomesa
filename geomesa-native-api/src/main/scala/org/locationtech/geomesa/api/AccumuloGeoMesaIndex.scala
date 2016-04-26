package org.locationtech.geomesa.api

import java.lang.Iterable
import java.util.Date

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.data.{DataStoreFinder, Transaction}
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.utils.geotools.{Conversions, SftBuilder}

class AccumuloGeoMesaIndex[T](ds: AccumuloDataStore,
                              fname: String,
                              serde: ValueSerializer[T]) extends GeoMesaIndex[T] {

  if(!ds.getTypeNames.contains(fname)) {
    val sft =
      new SftBuilder()
        .date("dtg", default = true, index = true)
        .bytes("payload")
        .geometry("geom", default = true)
        .withIndexes(List(Z3Table.suffix))
        .build(fname)
    ds.createSchema(sft)
  }

  val fs = ds.getFeatureSource(fname)

  val writers =
    CacheBuilder.newBuilder().build(
      new CacheLoader[String, SimpleFeatureWriter] {
        override def load(k: String): SimpleFeatureWriter = {
          ds.getFeatureWriterAppend(k, Transaction.AUTO_COMMIT).asInstanceOf[SimpleFeatureWriter]
        }
      })

  override def query(query: GeoMesaQuery): Iterable[T] = {
    import Conversions._

    import scala.collection.JavaConverters._

    fs.getFeatures(query.getFilter)
      .features()
      .map { f => serde.fromBytes(f.getAttribute(1).asInstanceOf[Array[Byte]]) }
      .toIterable.asJava
  }

  override def put(t: T, geom: Geometry, dtg: Date): Unit = {
    val bytes = serde.toBytes(t)
    val fw = writers.get(fname)
    val sf = fw.next()
    sf.setDefaultGeometry(geom)
    sf.setAttribute(0, dtg)
    sf.setAttribute(1, bytes)
    fw.write()
  }

  override def delete(t: T): Unit = ???
}

object AccumuloGeoMesaIndex {
  def build[T](name: String, zk: String, instanceId: String, user: String, pass: String, mock: Boolean,
               valueSerializer: ValueSerializer[T]) = {
    import scala.collection.JavaConversions._
    val ds =
      DataStoreFinder.getDataStore(Map(
        AccumuloDataStoreParams.tableNameParam.key -> name,
        AccumuloDataStoreParams.zookeepersParam.key -> zk,
        AccumuloDataStoreParams.instanceIdParam.key -> instanceId,
        AccumuloDataStoreParams.userParam.key -> user,
        AccumuloDataStoreParams.passwordParam.key -> pass,
        AccumuloDataStoreParams.mockParam.key -> (if(mock) "TRUE" else "FALSE")
      )).asInstanceOf[AccumuloDataStore]
    new AccumuloGeoMesaIndex[T](ds, name, valueSerializer)
  }
}
