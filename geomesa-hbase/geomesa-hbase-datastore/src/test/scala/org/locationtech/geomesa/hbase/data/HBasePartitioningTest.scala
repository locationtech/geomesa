/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.conf.partition.{TablePartition, TimePartition}
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.index.utils.ExplainPrintln
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.date.DateUtils.toInstant
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.MatchResult

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class HBasePartitioningTest extends HBaseTest with LazyLogging {

  sequential

  step {
    logger.info("Starting the HBase partitioning test")
  }

  "HBaseDataStore" should {
    "partition tables based on feature date" in {
      val typeName = "testpartition"
      val spec = "name:String:index=true,attr:String,dtg:Date,*geom:Point:srid=4326;"

      val params = Map(ConnectionParam.getName -> connection, HBaseCatalogParam.getName -> catalogTableName)
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
      ds must not(beNull)

      try {
        ds.getSchema(typeName) must beNull

        ds.createSchema(SimpleFeatureTypes.createType(typeName,
          s"$spec${Configs.TABLE_PARTITIONING}=${TimePartition.Name}"))

        val sft = ds.getSchema(typeName)

        sft must not(beNull)

        ds.getAllIndexTableNames(typeName) must beEmpty

        val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

        val toAdd = (0 until 10).map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          sf.setAttribute(0, s"name$i")
          sf.setAttribute(1, s"name$i")
          sf.setAttribute(2, f"2018-01-${i + 1}%02dT00:00:01.000Z")
          sf.setAttribute(3, s"POINT(4$i 5$i)")
          sf
        }

        val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd.take(8)))
        ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 8).map(_.toString))

        val indices = ds.manager.indices(sft)
        indices.map(_.name) must containTheSameElementsAs(Seq(Z3Index.name, Z2Index.name, IdIndex.name, AttributeIndex.name))
        foreach(indices)(i => i.getTableNames(None) must haveLength(2))

        // add the last two features to an alternate table and adopt them
        ds.createSchema(SimpleFeatureTypes.createType("testpartitionadoption", spec))
        WithClose(ds.getFeatureWriterAppend("testpartitionadoption", Transaction.AUTO_COMMIT)) { writer =>
          FeatureUtils.copyToWriter(writer, toAdd(8), useProvidedFid = true)
          writer.write()
          FeatureUtils.copyToWriter(writer, toAdd(9), useProvidedFid = true)
          writer.write()
        }
        // duplicates the logic in `org.locationtech.geomesa.tools.data.ManagePartitionsCommand.AdoptPartitionCommand`
        ds.manager.indices(ds.getSchema("testpartitionadoption")).foreach { index =>
          val table = index.getTableNames(None).head
          ds.metadata.insert(sft.getTypeName, index.tableNameKey(Some("foo")), table)
        }
        def zonedDateTime(sf: SimpleFeature) =
          ZonedDateTime.ofInstant(toInstant(sf.getAttribute("dtg").asInstanceOf[Date]), ZoneOffset.UTC)
        TablePartition(ds, sft).get.asInstanceOf[TimePartition].register("foo", zonedDateTime(toAdd(8)), zonedDateTime(toAdd(9)))

        // verify the table was adopted
        foreach(indices)(i => i.getTableNames(None) must haveLength(3))

        val transformsList = Seq(null, Array("geom"), Array("geom", "dtg"), Array("name"), Array("dtg", "geom", "attr", "name"))

        foreach(transformsList) { transforms =>
          testQuery(ds, typeName, "IN('0', '2')", transforms, Seq(toAdd(0), toAdd(2)))
          testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2018-01-01T00:00:00.000Z/2018-01-08T12:00:00.000Z", transforms, toAdd.dropRight(2))
          testQuery(ds, typeName, "bbox(geom,42,48,52,62) and dtg DURING 2017-12-15T00:00:00.000Z/2018-01-15T00:00:00.000Z", transforms, toAdd.drop(2))
          testQuery(ds, typeName, "bbox(geom,42,48,52,62)", transforms, toAdd.drop(2))
          testQuery(ds, typeName, "dtg DURING 2018-01-01T00:00:00.000Z/2018-01-08T12:00:00.000Z", transforms, toAdd.dropRight(2))
          testQuery(ds, typeName, "attr = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2018-01-01T00:00:00.000Z/2018-01-08T12:00:00.000Z", transforms, Seq(toAdd(5)))
          testQuery(ds, typeName, "name < 'name5'", transforms, toAdd.take(5))
          testQuery(ds, typeName, "name = 'name5'", transforms, Seq(toAdd(5)))
        }

        ds.getFeatureSource(typeName).removeFeatures(ECQL.toFilter("INCLUDE"))

        forall(Seq("INCLUDE",
          "IN('0', '2')",
          "bbox(geom,42,48,52,62)",
          "bbox(geom,38,48,52,62) and dtg DURING 2018-01-01T00:00:00.000Z/2018-01-08T12:00:00.000Z",
          "bbox(geom,42,48,52,62) and dtg DURING 2017-12-15T00:00:00.000Z/2018-01-15T00:00:00.000Z",
          "dtg DURING 2018-01-01T00:00:00.000Z/2018-01-08T12:00:00.000Z",
          "attr = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2018-01-01T00:00:00.000Z/2018-01-08T12:00:00.000Z",
          "name < 'name5'",
          "name = 'name5'")) { filter =>
          val fr = ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter)), Transaction.AUTO_COMMIT)
          SelfClosingIterator(fr).toList must beEmpty
        }
      } finally {
        ds.dispose()
      }
    }
  }

  def testQuery(ds: HBaseDataStore, typeName: String, filter: String, transforms: Array[String], results: Seq[SimpleFeature]): MatchResult[Any] = {
    val query = new Query(typeName, ECQL.toFilter(filter), transforms)
    val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    val features = SelfClosingIterator(fr).toList
    if (features.length != results.length) {
      ds.getQueryPlan(query, explainer = new ExplainPrintln)
    }
    val attributes = Option(transforms).getOrElse(ds.getSchema(typeName).getAttributeDescriptors.map(_.getLocalName).toArray)
    features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
    forall(features) { feature =>
      feature.getAttributes must haveLength(attributes.length)
      forall(attributes.zipWithIndex) { case (attribute, i) =>
        feature.getAttribute(attribute) mustEqual feature.getAttribute(i)
        feature.getAttribute(attribute) mustEqual results.find(_.getID == feature.getID).get.getAttribute(attribute)
      }
    }
    QueryProperties.QueryExactCount.threadLocalValue.set("true")
    try {
      ds.getFeatureSource(typeName).getFeatures(query).size() mustEqual results.length
    } finally {
      QueryProperties.QueryExactCount.threadLocalValue.remove()
    }
  }
}
