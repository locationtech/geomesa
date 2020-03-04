/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.MiniCluster.UserWithAuths
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
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

  def spec: String

  // we use class name to prevent spillage between unit tests in the mock connector
  lazy val sftName = getClass.getSimpleName
  lazy val catalog = s"${MiniCluster.namespace}.$sftName"

  // note the table needs to be different to prevent tests from conflicting with each other
  lazy val dsParams = MiniCluster.params ++ Map(AccumuloDataStoreParams.CatalogParam.key -> catalog)

  lazy val ds = {
    val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]
    ds.createSchema(SimpleFeatureTypes.createType(sftName, spec))
    ds
  }

  lazy val fs = ds.getFeatureSource(sftName)

  lazy val sft = ds.getSchema(sftName) // reload the sft from the ds to ensure all user data is set properly

  lazy val root  = MiniCluster.root
  lazy val admin = MiniCluster.admin
  lazy val user  = MiniCluster.user

  override def map(fragments: => Fragments): Fragments = fragments ^ fragmentFactory.step {
    // TODO time this
    // ds.removeSchema(sftName)
    ds.dispose()
  }

  /**
   * Write a feature to the data store
   *
   * @param feature feature to write, will use provided fid
   */
  def addFeature(feature: SimpleFeature): Unit = addFeatures(Seq(feature))

  /**
   * Writes features to the data store
   *
   * @param features features to write, will use provided fid
   */
  def addFeatures(features: Seq[SimpleFeature]): Unit = {
    WithClose(ds.getFeatureWriterAppend(sftName, Transaction.AUTO_COMMIT)) { writer =>
      features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
    }
  }

  /**
   * Deletes all existing features
   */
  def clearFeatures(): Unit = {
    val writer = ds.getFeatureWriter(sftName, Filter.INCLUDE, Transaction.AUTO_COMMIT)
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

  def explain(filter: String): String = explain(new Query(sftName, ECQL.toFilter(filter)))

  def rowToString(key: Key) = bytesToString(key.getRow.copyBytes())

  def bytesToString(bytes: Array[Byte]) = Key.toPrintableString(bytes, 0, bytes.length, bytes.length)
}
