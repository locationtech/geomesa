/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import java.util.concurrent.atomic.AtomicInteger

import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragments

import scala.collection.JavaConverters._

/**
 * Trait to simplify tests that require reading and writing features from an AccumuloDataStore
 */
trait TestWithMultipleSfts extends Specification {

  private val sftCounter = new AtomicInteger(0)

  // we use class name to prevent spillage between unit tests in the mock connector
  lazy val catalog = s"${MiniCluster.namespace}.${getClass.getSimpleName}"

  // note the table needs to be different to prevent tests from conflicting with each other
  lazy val dsParams = MiniCluster.params ++ Map(AccumuloDataStoreParams.CatalogParam.key -> catalog)

  lazy val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]

  lazy val root  = MiniCluster.root
  lazy val admin = MiniCluster.admin
  lazy val user  = MiniCluster.user

  // after all tests, drop the tables we created to free up memory
  override def map(fragments: => Fragments): Fragments = fragments ^ fragmentFactory.step {
// TODO time this
    //  ds.delete()
    ds.dispose()
  }

  /**
   * Create a new schema
   *
   * @param spec simple feature type spec
   * @return
   */
  def createNewSchema(spec: String): SimpleFeatureType = synchronized {
    val sftName = s"$catalog${sftCounter.getAndIncrement()}"
    ds.createSchema(SimpleFeatureTypes.createType(sftName, spec))
    ds.getSchema(sftName) // reload the sft from the ds to ensure all user data is set properly
  }

  /**
   * Write a feature to the data store
   *
   * @param sft feature type
   * @param feature feature to write, will use provided fid
   */
  def addFeature(sft: SimpleFeatureType, feature: SimpleFeature): Unit = addFeatures(sft, Seq(feature))

  /**
   * Writes features to the data store
   *
   * @param sft feature type
   * @param features features to write, will use provided fid
   * @return feature ids that were added
   */
  def addFeatures(sft: SimpleFeatureType, features: Seq[SimpleFeature]): Unit = {
    WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
      features.foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
    }
  }

  /**
   * Deletes all existing features for the given feature type
   *
   * @param sft simple feature type
   */
  def clearFeatures(sft: SimpleFeatureType): Unit = {
    val writer = ds.getFeatureWriter(sft.getTypeName, Filter.INCLUDE, Transaction.AUTO_COMMIT)
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
}
