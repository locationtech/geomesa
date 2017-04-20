/** *********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
* ************************************************************************/
package org.locationtech.geomesa.hbase.nativeapi

import org.apache.hadoop.classification.InterfaceStability
import org.apache.hadoop.hbase.client.Connection
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.api._
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseDataStoreParams, HBaseFeatureWriter}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@InterfaceStability.Unstable
class HBaseGeoIndex[T](override protected val ds: HBaseDataStore,
                       name: String,
                       serde: ValueSerializer[T],
                       view: SimpleFeatureView[T]) extends BaseBigTableIndex[T](ds, name, serde, view) {

  override def flush(): Unit = {
    writers.asMap().values().map(_.asInstanceOf[HBaseFeatureWriter]).foreach(_.flush())
  }
}

@InterfaceStability.Unstable
object HBaseGeoIndex {
  def build[T](name: String,
               remote: Boolean,
               connection: Connection,
               valueSerializer: ValueSerializer[T]): HBaseGeoIndex[T] = {
    build(name, remote, connection, valueSerializer, new DefaultSimpleFeatureView[T]())
  }

  def build[T](name: String,
               remote: Boolean,
               connection: Connection,
               valueSerializer: ValueSerializer[T],
               view: SimpleFeatureView[T]): HBaseGeoIndex[T] =
    buildWithView[T](name, remote, connection, valueSerializer, view)

  def build[T](name: String,
               remote: Boolean,
               valueSerializer: ValueSerializer[T])
              (view: SimpleFeatureView[T] = new DefaultSimpleFeatureView[T]()): HBaseGeoIndex[T] =
    buildWithView[T](name, remote, valueSerializer, view)

  def buildWithView[T](name: String,
                       remote: Boolean,
                       valueSerializer: ValueSerializer[T],
                       view: SimpleFeatureView[T]): HBaseGeoIndex[T] = {
    import scala.collection.JavaConversions._
    val ds =
      DataStoreFinder.getDataStore(Map(
        HBaseDataStoreParams.BigTableNameParam.key-> name,
        HBaseDataStoreParams.RemoteParam.key-> remote
      )).asInstanceOf[HBaseDataStore]
    new HBaseGeoIndex[T](ds, name, valueSerializer, view)
  }

  def buildWithView[T](name: String,
                       remote: Boolean,
                       connection: Connection,
                       valueSerializer: ValueSerializer[T],
                       view: SimpleFeatureView[T]): HBaseGeoIndex[T] = {

    val ds = DataStoreFinder.getDataStore(
      Map[String, Any](
        HBaseDataStoreParams.ConnectionParam.key -> connection,
        HBaseDataStoreParams.BigTableNameParam.key-> name,
        HBaseDataStoreParams.RemoteParam.key-> remote
    ).asJava).asInstanceOf[HBaseDataStore]
    new HBaseGeoIndex[T](ds, name, valueSerializer, view)
  }

  def buildDefaultView[T](name: String,
                          remote: Boolean,
                          valueSerializer: ValueSerializer[T]): HBaseGeoIndex[T] = {
    build(name, remote, valueSerializer)()
  }

  def buildDefaultView[T](name: String,
                          remote: Boolean,
                          connection: Connection,
                          valueSerializer: ValueSerializer[T]): HBaseGeoIndex[T] = {
    build(name, remote, connection, valueSerializer, new DefaultSimpleFeatureView[T]())
  }

}