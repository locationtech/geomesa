/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.process

import org.geomesa.testcontainers.AccumuloContainer
import org.geotools.api.data.{DataStoreFinder, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragments

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

/**
 * Trait to simplify tests that require reading and writing features from an AccumuloDataStore
 */
trait TestWithDataStore extends Specification {

  private val sftCounter = new AtomicInteger(0)

  def spec: String

  lazy val sft = {
    // we use class name to prevent spillage between unit tests
    ds.createSchema(SimpleFeatureTypes.createType(getClass.getSimpleName, spec))
    ds.getSchema(getClass.getSimpleName) // reload the sft from the ds to ensure all user data is set properly
  }

  lazy val sftName = sft.getTypeName

  lazy val fs = ds.getFeatureSource(sftName)

  // we use class name to prevent spillage between unit tests
  // use different namespaces to verify namespace creation works correctly
  lazy val catalog = s"${getClass.getSimpleName.take(2)}.${getClass.getSimpleName}"

  // note the table needs to be different to prevent tests from conflicting with each other
  lazy val dsParams: Map[String, String] = Map(
    AccumuloDataStoreParams.InstanceNameParam.key -> AccumuloContainer.getInstance().getInstanceName,
    AccumuloDataStoreParams.ZookeepersParam.key   -> AccumuloContainer.getInstance().getZookeepers,
    AccumuloDataStoreParams.UserParam.key         -> AccumuloContainer.getInstance().getUsername,
    AccumuloDataStoreParams.PasswordParam.key     -> AccumuloContainer.getInstance().getPassword,
    AccumuloDataStoreParams.CatalogParam.key      -> catalog
  )

  lazy val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]

  override def map(fragments: => Fragments): Fragments = fragments ^ fragmentFactory.step {
    ds.delete()
    ds.dispose()
  }

  /**
   * Create a new schema
   *
   * @param spec simple feature type spec
   * @return
   */
  def createNewSchema(spec: String): SimpleFeatureType = {
    val sftName = s"${getClass.getSimpleName}${sftCounter.getAndIncrement()}"
    ds.createSchema(SimpleFeatureTypes.createType(sftName, spec))
    ds.getSchema(sftName) // reload the sft from the ds to ensure all user data is set properly
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
}
