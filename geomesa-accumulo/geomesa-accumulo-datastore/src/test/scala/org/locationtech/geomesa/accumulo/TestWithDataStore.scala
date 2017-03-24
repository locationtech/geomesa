/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import org.apache.accumulo.core.client.Scanner
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.specification.{Fragments, Step}

import scala.collection.JavaConverters._

/**
 * Trait to simplify tests that require reading and writing features from an AccumuloDataStore
 */
trait TestWithDataStore extends Specification {

  def spec: String
  def dtgField: Option[String] = Some("dtg")

  // TODO GEOMESA-1146 refactor to allow running of tests with table sharing on and off...
  def tableSharing: Boolean = true

  // we use class name to prevent spillage between unit tests in the mock connector
  lazy val sftName = getClass.getSimpleName

  val EmptyUserAuthorizations = new Authorizations()

  val MockUserAuthorizationsString = "A,B,C"
  val MockUserAuthorizations = new Authorizations(
    MockUserAuthorizationsString.split(",").map(_.getBytes()).toList.asJava
  )

  lazy val mockInstanceId = "mycloud"
  lazy val mockUser = "user"
  lazy val mockPassword = "password"
  // assign some default authorizations to this mock user
  lazy val connector = {
    val mockInstance = new MockInstance(mockInstanceId)
    val mockConnector = mockInstance.getConnector(mockUser, new PasswordToken(mockPassword))
    mockConnector.securityOperations().changeUserAuthorizations(mockUser, MockUserAuthorizations)
    mockConnector
  }

  lazy val dsParams = Map(
    "connector" -> connector,
    "caching"   -> false,
    // note the table needs to be different to prevent testing errors
    "tableName" -> sftName
  )

  lazy val (ds, sft) = {
    val sft = SimpleFeatureTypes.createType(sftName, spec)
    sft.setTableSharing(tableSharing)
    dtgField.foreach(sft.setDtgField)
    val ds = DataStoreFinder.getDataStore(dsParams.asJava).asInstanceOf[AccumuloDataStore]
    ds.createSchema(sft)
    (ds, ds.getSchema(sftName)) // reload the sft from the ds to ensure all user data is set properly
  }

  lazy val fs = ds.getFeatureSource(sftName)

  // after all tests, drop the tables we created to free up memory
  override def map(fragments: => Fragments) = fragments ^ Step {
    ds.removeSchema(sftName)
  }

  /**
   * Call to load the test features into the data store
   */
  def addFeatures(features: Seq[SimpleFeature]): Unit = {
    val featureCollection = new DefaultFeatureCollection(sftName, sft)
    features.foreach { f =>
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      featureCollection.add(f)
    }
    // write the feature to the store
    fs.addFeatures(featureCollection)
  }

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

  def scanner(table: AccumuloFeatureIndex): Scanner =
    connector.createScanner(table.getTableName(sftName, ds), new Authorizations())

  def rowToString(key: Key) = bytesToString(key.getRow.copyBytes())

  def bytesToString(bytes: Array[Byte]) = Key.toPrintableString(bytes, 0, bytes.length, bytes.length)
}
