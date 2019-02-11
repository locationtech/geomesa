/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import java.util.concurrent.atomic.AtomicInteger

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.identity.FeatureId
import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragments

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Trait to simplify tests that require reading and writing features from an AccumuloDataStore
 */
trait TestWithMultipleSfts extends Specification {

  // we use class name to prevent spillage between unit tests in the mock connector
  protected val sftBaseName = getClass.getSimpleName
  private val sftCounter = new AtomicInteger(0)
  private val sfts = ArrayBuffer.empty[SimpleFeatureType]

  val connector = new MockInstance("mycloud").getConnector("user", new PasswordToken("password"))

  val dsParams = Map(
    AccumuloDataStoreParams.ConnectorParam.key -> connector,
    AccumuloDataStoreParams.CachingParam.key   -> false,
    // note the table needs to be different to prevent testing errors
    AccumuloDataStoreParams.CatalogParam.key   -> sftBaseName)

  val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]

  // after all tests, drop the tables we created to free up memory
  override def map(fragments: => Fragments): Fragments = fragments ^ fragmentFactory.step {
    ds.delete()
    ds.dispose()
  }

  def createNewSchema(spec: String, dtgField: Option[String] = Some("dtg")): SimpleFeatureType = synchronized {
    val sftName = sftBaseName + sftCounter.getAndIncrement()
    val sft = SimpleFeatureTypes.createType(sftName, spec)
    dtgField.foreach(sft.setDtgField)
    ds.createSchema(sft)
    val reloaded = ds.getSchema(sftName) // reload the sft from the ds to ensure all user data is set properly
    sfts += reloaded
    reloaded
  }

  def addFeature(sft: SimpleFeatureType, feature: SimpleFeature): java.util.List[FeatureId] =
    addFeatures(sft, Seq(feature))

  /**
   * Call to load the test features into the data store
   */
  def addFeatures(sft: SimpleFeatureType, features: Seq[SimpleFeature]): java.util.List[FeatureId] = {
    val featureCollection = new DefaultFeatureCollection(sft.getTypeName, sft)
    features.foreach { f =>
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      featureCollection.add(f)
    }
    // write the feature to the store
    ds.getFeatureSource(sft.getTypeName).addFeatures(featureCollection)
  }

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
