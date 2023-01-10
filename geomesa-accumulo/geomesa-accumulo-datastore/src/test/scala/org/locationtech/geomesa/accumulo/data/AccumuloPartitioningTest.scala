/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.api.data._
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.conf.partition.TimePartition
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.utils.ExplainPrintln
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloPartitioningTest extends Specification with TestWithFeatureType {

  import scala.collection.JavaConverters._

  // note: using `Seq.foreach; ok` instead of `foreach(Seq)` shaves several seconds off the time to run this test

  override val spec: String =
    s"name:String:index=true,attr:String,dtg:Date,*geom:Point:srid=4326;${Configs.TablePartitioning}=${TimePartition.Name}"

  lazy val parallelDs = {
    val params = dsParams + (AccumuloDataStoreParams.PartitionParallelScansParam.key -> "true")
    DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]
  }

  val features = (0 until 10).map { i =>
    val sf = new ScalaSimpleFeature(sft, i.toString)
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf.setAttribute(0, s"name$i")
    sf.setAttribute(1, s"name$i")
    sf.setAttribute(2, f"2018-01-${i + 1}%02dT00:00:01.000Z")
    sf.setAttribute(3, s"POINT(4$i 5$i)")
    sf
  }

  "AccumuloDataStore" should {
    "partition tables based on feature date" in {
      ds.getAllIndexTableNames(sftName) must beEmpty
      addFeatures(features)

      val indices = ds.manager.indices(sft)
      indices.map(_.name) must containTheSameElementsAs(Seq(Z3Index.name, Z2Index.name, IdIndex.name, AttributeIndex.name))
      foreach(indices)(_.getTableNames(None) must haveLength(2))

      val transformsList = Seq(null, Array("geom"), Array("geom", "dtg"), Array("name"), Array("dtg", "geom", "attr", "name"))

      transformsList.foreach { transforms =>
        testQuery("IN('0', '2')", transforms, Seq(features(0), features(2)))
        testQuery("bbox(geom,38,48,52,62) and dtg DURING 2018-01-01T00:00:00.000Z/2018-01-08T12:00:00.000Z", transforms, features.dropRight(2))
        testQuery("bbox(geom,42,48,52,62) and dtg DURING 2017-12-15T00:00:00.000Z/2018-01-15T00:00:00.000Z", transforms, features.drop(2))
        testQuery("bbox(geom,42,48,52,62)", transforms, features.drop(2))
        testQuery("dtg DURING 2018-01-01T00:00:00.000Z/2018-01-08T12:00:00.000Z", transforms, features.dropRight(2))
        testQuery("attr = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2018-01-01T00:00:00.000Z/2018-01-08T12:00:00.000Z", transforms, Seq(features(5)))
        testQuery("name < 'name5'", transforms, features.take(5))
        testQuery("name = 'name5'", transforms, Seq(features(5)))
      }

      ds.getFeatureSource(sftName).removeFeatures(ECQL.toFilter("INCLUDE"))

      foreach(indices)(_.getTableNames(None) must beEmpty)

      Seq("INCLUDE",
        "IN('0', '2')",
        "bbox(geom,42,48,52,62)",
        "bbox(geom,38,48,52,62) and dtg DURING 2018-01-01T00:00:00.000Z/2018-01-08T12:00:00.000Z",
        "bbox(geom,42,48,52,62) and dtg DURING 2017-12-15T00:00:00.000Z/2018-01-15T00:00:00.000Z",
        "dtg DURING 2018-01-01T00:00:00.000Z/2018-01-08T12:00:00.000Z",
        "attr = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2018-01-01T00:00:00.000Z/2018-01-08T12:00:00.000Z",
        "name < 'name5'",
        "name = 'name5'").foreach { filter =>
        val fr = ds.getFeatureReader(new Query(sftName, ECQL.toFilter(filter)), Transaction.AUTO_COMMIT)
        SelfClosingIterator(fr).toList must beEmpty
      }
      ok
    }
  }

  def testQuery(filter: String, transforms: Array[String], results: Seq[SimpleFeature]): Unit = {
<<<<<<< HEAD
    foreach(Seq(ds, parallelDs)) { ds =>
      val query = new Query(sftName, ECQL.toFilter(filter), transforms: _*)
      val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
      val features = SelfClosingIterator(fr).toList
      if (features.length != results.length) {
        ds.getQueryPlan(query, explainer = new ExplainPrintln)
=======
    val query = new Query(sftName, ECQL.toFilter(filter), transforms: _*)
    val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    val features = SelfClosingIterator(fr).toList
    if (features.length != results.length) {
      ds.getQueryPlan(query, explainer = new ExplainPrintln)
    }
    val attributes = Option(transforms).getOrElse(ds.getSchema(sftName).getAttributeDescriptors.asScala.map(_.getLocalName).toArray)
    features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
    forall(features) { feature =>
      feature.getAttributes must haveLength(attributes.length)
      forall(attributes.zipWithIndex) { case (attribute, i) =>
        feature.getAttribute(attribute) mustEqual feature.getAttribute(i)
        feature.getAttribute(attribute) mustEqual results.find(_.getID == feature.getID).get.getAttribute(attribute)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
      }
      val attributes = Option(transforms).getOrElse(ds.getSchema(sftName).getAttributeDescriptors.asScala.map(_
          .getLocalName).toArray)
      features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
      forall(features) { feature =>
        feature.getAttributes must haveLength(attributes.length)
        forall(attributes.zipWithIndex) { case (attribute, i) => feature.getAttribute(attribute) mustEqual
            feature.getAttribute(i)
          feature.getAttribute(attribute) mustEqual results.find(_.getID == feature.getID).get.getAttribute(attribute)
        }
      }
      query.getHints.put(QueryHints.EXACT_COUNT, java.lang.Boolean.TRUE)
      ds.getFeatureSource(sftName).getFeatures(query).size() mustEqual results.length
    }
  }
}
