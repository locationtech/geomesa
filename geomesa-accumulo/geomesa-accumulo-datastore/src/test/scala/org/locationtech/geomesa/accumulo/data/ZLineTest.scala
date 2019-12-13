/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.index.IndexMode
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ZLineTest extends Specification with TestWithDataStore {

  sequential

  override val spec = "name:String,dtg:Date,*geom:LineString:srid=4326"

  addFeatures({
    val sf = new ScalaSimpleFeature(sft, "fid1")
    sf.setAttribute("name", "fred")
    sf.setAttribute("dtg", "2015-01-01T12:00:00.000Z")
    sf.setAttribute("geom", "LINESTRING(47.28515625 25.576171875, 48 26, 49 27)")
    Seq(sf)
  })

  def printR(e: java.util.Map.Entry[Key, Value]): Unit = {
    val row = Key.toPrintableString(e.getKey.getRow.getBytes, 0, e.getKey.getRow.getLength, e.getKey.getRow.getLength)
    val cf = e.getKey.getColumnFamily.toString
    val cq = e.getKey.getColumnQualifier.getBytes.map("%02X" format _).mkString
    val value = Key.toPrintableString(e.getValue.get(), 0, e.getValue.getSize, e.getValue.getSize)
    println(s"$row :: $cf :: $cq :: $value")
  }

  "ZLines" should {
    "add features" in {
      skipped("testing")
      new Z3Index(ds, sft, "geom", "dtg", IndexMode.ReadWrite).getTableNames().foreach { table =>
        println(table)
        val scanner = ds.connector.createScanner(table, new Authorizations())
        println(scanner.toSeq.length)
        scanner.close()
      }
      success
    }
    "return features that are contained" in {
      val filter = "bbox(geom,47,25,50,28) and dtg DURING 2015-01-01T11:00:00.000Z/2015-01-01T13:00:00.000Z"
      val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
      val features = SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures(query).features).toList
      features must haveLength(1)
      features.head.getID mustEqual "fid1"
    }
    "return features that intersect" in {
      val filter = "bbox(geom,47.5,25,49,26) and dtg DURING 2015-01-01T11:00:00.000Z/2015-01-01T13:00:00.000Z"
      val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
      val features = SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures(query).features).toList
      features must haveLength(1)
      features.head.getID mustEqual "fid1"
    }
    "not return features that don't intersect" in {
      val filter = "bbox(geom,45,24,46,25) and dtg DURING 2015-01-01T11:00:00.000Z/2015-01-01T13:00:00.000Z"
      val query = new Query(sft.getTypeName, ECQL.toFilter(filter))
      val features = SelfClosingIterator(ds.getFeatureSource(sft.getTypeName).getFeatures(query).features).toList
      features must beEmpty
    }
  }
}
