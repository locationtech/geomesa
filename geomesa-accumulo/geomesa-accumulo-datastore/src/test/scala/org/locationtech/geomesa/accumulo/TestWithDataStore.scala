/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import org.apache.accumulo.core.data.Key
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragments

import scala.collection.JavaConverters._

/**
 * Trait to simplify tests that require reading and writing features from an AccumuloDataStore
 */
trait TestWithDataStore extends Specification {

  // we use class name to prevent spillage between unit tests
  lazy val catalog = s"${MiniCluster.namespace}.${getClass.getSimpleName}"

  // note the table needs to be different to prevent tests from conflicting with each other
  lazy val dsParams: Map[String, String] = Map(
    AccumuloDataStoreParams.InstanceIdParam.key -> MiniCluster.cluster.getInstanceName,
    AccumuloDataStoreParams.ZookeepersParam.key -> MiniCluster.cluster.getZooKeepers,
    AccumuloDataStoreParams.UserParam.key       -> MiniCluster.Users.root.name,
    AccumuloDataStoreParams.PasswordParam.key   -> MiniCluster.Users.root.password,
    AccumuloDataStoreParams.CatalogParam.key    -> catalog
  )

  lazy val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]

  lazy val root  = MiniCluster.Users.root
  lazy val admin = MiniCluster.Users.admin
  lazy val user  = MiniCluster.Users.user

  override def map(fragments: => Fragments): Fragments = fragments ^ fragmentFactory.step {
    ds.delete()
    ds.dispose()
  }

  /**
   * Write a feature to the data store
   *
   * @param feature feature to write, will use provided fid
   */
  def addFeature(feature: SimpleFeature): Unit = addFeatures(Seq(feature))

  /**
   * Writes features to the data store. All features must be of the same feature type
   *
   * @param features features to write, will use provided fid
   */
  def addFeatures(features: Seq[SimpleFeature]): Unit = {
    if (features.nonEmpty) {
      val typeName = features.head.getFeatureType.getTypeName
      WithClose(ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)) { writer =>
        features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }
    }
  }

  /**
   * Deletes all existing features
   */
  def clearFeatures(typeName: String): Unit = {
    val writer = ds.getFeatureWriter(typeName, Filter.INCLUDE, Transaction.AUTO_COMMIT)
    while (writer.hasNext) {
      writer.next()
      writer.remove()
    }
    writer.close()
  }

  def explain(query: Query): String = {
    val o = new ExplainString
    ds.getQueryPlan(query, explainer = o)
    o.toString()
  }

  def rowToString(key: Key) = bytesToString(key.getRow.copyBytes())

  def bytesToString(bytes: Array[Byte]) = Key.toPrintableString(bytes, 0, bytes.length, bytes.length)
}
