/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.nativeapi

import org.apache.accumulo.core.client.Connector
import org.apache.hadoop.classification.InterfaceStability
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.api._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@InterfaceStability.Unstable
@deprecated("Will be removed without replacement")
class AccumuloGeoMesaIndex[T](override protected val ds: AccumuloDataStore,
                              name: String,
                              serde: ValueSerializer[T],
                              view: SimpleFeatureView[T]) extends BaseBigTableIndex[T](ds, name, serde, view) {
  override def flush(): Unit = {
    writers.asMap().values().foreach(_.flush())
  }
}

@InterfaceStability.Unstable
@deprecated("Will be removed without replacement")
object AccumuloGeoMesaIndex {
  def build[T](name: String,
               connector: Connector,
               valueSerializer: ValueSerializer[T]): AccumuloGeoMesaIndex[T] = {
    build(name, connector, valueSerializer, new DefaultSimpleFeatureView[T]())
  }

  def build[T](name: String,
               connector: Connector,
               valueSerializer: ValueSerializer[T],
               view: SimpleFeatureView[T]): AccumuloGeoMesaIndex[T] =
    buildWithView[T](name, connector, valueSerializer, view)

  def build[T](name: String,
               zk: String,
               instanceId: String,
               user: String, pass: String,
               mock: Boolean,
               valueSerializer: ValueSerializer[T])
              (view: SimpleFeatureView[T] = new DefaultSimpleFeatureView[T]()): AccumuloGeoMesaIndex[T] =
    buildWithView[T](name, zk, instanceId, user, pass, mock, valueSerializer, view)

  def buildWithView[T](name: String,
                       zk: String,
                       instanceId: String,
                       user: String, pass: String,
                       mock: Boolean,
                       valueSerializer: ValueSerializer[T],
                       view: SimpleFeatureView[T]): AccumuloGeoMesaIndex[T] = {
    import scala.collection.JavaConversions._
    val ds =
      DataStoreFinder.getDataStore(Map(
        AccumuloDataStoreParams.CatalogParam.key    -> name,
        AccumuloDataStoreParams.ZookeepersParam.key -> zk,
        AccumuloDataStoreParams.InstanceIdParam.key -> instanceId,
        AccumuloDataStoreParams.UserParam.key       -> user,
        AccumuloDataStoreParams.PasswordParam.key   -> pass,
        AccumuloDataStoreParams.MockParam.key       -> (if (mock) "TRUE" else "FALSE")
      )).asInstanceOf[AccumuloDataStore]
    new AccumuloGeoMesaIndex[T](ds, name, valueSerializer, view)
  }

  def buildWithView[T](name: String,
                       connector: Connector,
                       valueSerializer: ValueSerializer[T],
                       view: SimpleFeatureView[T]): AccumuloGeoMesaIndex[T] = {

    val ds = DataStoreFinder.getDataStore(
      Map[String, java.io.Serializable](
        AccumuloDataStoreParams.ConnectorParam.key -> connector.asInstanceOf[java.io.Serializable],
        AccumuloDataStoreParams.CatalogParam.key   -> name
      ).asJava).asInstanceOf[AccumuloDataStore]
    new AccumuloGeoMesaIndex[T](ds, name, valueSerializer, view)
  }

  def buildDefaultView[T](name: String,
                          zk: String,
                          instanceId: String,
                          user: String, pass: String,
                          mock: Boolean,
                          valueSerializer: ValueSerializer[T]): AccumuloGeoMesaIndex[T] = {
    build(name, zk, instanceId, user, pass, mock, valueSerializer)()
  }

  def buildDefaultView[T](name: String,
                          connector: Connector,
                          valueSerializer: ValueSerializer[T]): AccumuloGeoMesaIndex[T] = {
    build(name, connector, valueSerializer, new DefaultSimpleFeatureView[T]())
  }

}
