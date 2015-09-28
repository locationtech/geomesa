/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.index

import cascading.tuple.Tuple
import com.twitter.scalding.{Args, Mode, Source}
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.io.Text
import org.geotools.data.DataStoreFinder
import org.geotools.feature.DefaultFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter.FeatureToWrite
import org.locationtech.geomesa.accumulo.data.tables.AttributeTable
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeatureStore}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.features.{ScalaSimpleFeatureFactory, SimpleFeatureSerializers}
import org.locationtech.geomesa.jobs.index.AttributeIndexJob._
import org.locationtech.geomesa.jobs.scalding.{AccumuloSource, ConnectionParams, GeoMesaSource}
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.IndexCoverage
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class AttributeIndexJobTest extends Specification {

  val tableName = "AttributeIndexJobTest"
  val params = Map(
    "instanceId"        -> "mycloud",
    "zookeepers"        -> "zoo1,zoo2,zoo3",
    "user"              -> "myuser",
    "password"          -> "mypassword",
    "auths"             -> "",
    "tableName"         -> tableName,
    "useMock"           -> "true")

  val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

  def test(schema: SimpleFeatureType, feats: Seq[SimpleFeature]) = {
    ds.createSchema(schema)
    val sft = ds.getSchema(schema.getTypeName)
    ds.getFeatureSource(sft.getTypeName).asInstanceOf[AccumuloFeatureStore].addFeatures {
      val collection = new DefaultFeatureCollection(sft.getTypeName, sft)
      collection.addAll(feats)
      collection
    }

    val jobParams = Map(ATTRIBUTES_TO_INDEX -> List("name"),
                        INDEX_COVERAGE -> List("join"),
                        ConnectionParams.FEATURE_IN -> List(sft.getTypeName))
    val scaldingArgs = new Args(ConnectionParams.toInArgs(params) ++ jobParams)

    val input = feats.map(f => new Tuple(new Text(f.getID), f)).toBuffer
    val output = mutable.Buffer.empty[Tuple]
    val buffers: (Source) => Option[mutable.Buffer[Tuple]] = {
      case gs: GeoMesaSource  => Some(input)
      case as: AccumuloSource => Some(output)
    }
    val arguments = Mode.putMode(com.twitter.scalding.Test(buffers), scaldingArgs)

    // create the output table - in non-mock environments this happens in the output format
    val tableOps = ds.connector.tableOperations
    val formattedTableName = AttributeTable.formatTableName(tableName, sft)
    if(!tableOps.exists(formattedTableName)) {
      tableOps.create(formattedTableName)
    }

    // run the job
    val job = new AttributeIndexJob(arguments)
    job.run must beTrue

    val indexValueEncoder = IndexValueEncoder(sft)
    val encoder = SimpleFeatureSerializers(sft, ds.getFeatureEncoding(sft))

    sft.getDescriptor("name").setIndexCoverage(IndexCoverage.JOIN)
    val writer = AttributeTable.writer(sft)
    val expectedMutations = feats.flatMap { sf =>
      val toWrite = new FeatureToWrite(sf, ds.writeVisibilities, encoder, indexValueEncoder)
      writer(toWrite)
    }

    val jobMutations = output.map(_.getObject(1).asInstanceOf[Mutation])
    jobMutations must containTheSameElementsAs(expectedMutations)
  }

  val spec = "name:String,dtg:Date,*geom:Point:srid=4326"
  def getTestFeatures(sft: SimpleFeatureType) = {
    (0 until 10).map { i =>
      val name = s"name$i"
      val dtg = s"2014-01-0${i}T00:00:00.000Z"
      val geom = s"POINT(${40 + i} ${60 - i})"
      ScalaSimpleFeatureFactory.buildFeature(sft, Array(name, dtg, geom), s"fid-$i")
    } ++ (10 until 20).map { i =>
      val name = null
      val dtg = s"2014-01-${i}T00:00:00.000Z"
      val geom = s"POINT(${40 + i} ${60 - i})"
      ScalaSimpleFeatureFactory.buildFeature(sft, Array(name, dtg, geom), s"fid-$i")
    }
  }

  "AccumuloIndexJob" should {
    "create the correct mutation for a stand-alone feature" in {
      val sft = SimpleFeatureTypes.createType("1", spec)
      sft.setTableSharing(false)
      test(sft, getTestFeatures(sft))
    }

    "create the correct mutation for a shared-table feature" in {
      val sft = SimpleFeatureTypes.createType("2", spec)
      sft.setTableSharing(true)
      test(sft, getTestFeatures(sft))
    }
  }
}
