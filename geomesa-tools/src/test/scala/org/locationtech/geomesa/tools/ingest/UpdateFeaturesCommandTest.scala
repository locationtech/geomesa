/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.tools.ingest

import com.beust.jcommander.ParameterException
import org.geotools.api.data.{DataStore, Query, Transaction}
import org.geotools.data.memory.MemoryDataStore
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.tools.ingest.UpdateFeaturesCommand.UpdateFeaturesParams
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.SpecificationWithJUnit

import java.util.Collections

class UpdateFeaturesCommandTest extends SpecificationWithJUnit {

  val sft = SimpleFeatureTypes.createType("tools", "name:String,level:String,dtg:Date,*geom:Point:srid=4326")
  val features = Seq.tabulate(10) { i =>
    ScalaSimpleFeature.create(sft, s"id$i", s"name$i", "user", s"2016-01-01T0$i:00:00.000Z", "POINT(1 0)")
  }

  def withCommand[T](ds: DataStore)(op: UpdateFeaturesCommand[DataStore] => T): T = {
    val command: UpdateFeaturesCommand[DataStore] = new UpdateFeaturesCommand[DataStore]() {
      override val params: UpdateFeaturesParams = new UpdateFeaturesParams(){}
      override def connection: Map[String, String] = Map.empty
      override def loadDataStore(): DataStore = ds
    }
    command.params.featureName = sft.getTypeName
    command.params.force = true
    op(command)
  }

  "UpdateFeaturesCommand" should {

    // note: can't easily test visibilities as memory store doesn't update them on write
    // that case is verified in AccumuloUpdateFeaturesCommandTest

    "validate input params" in {
      val ds = new MemoryDataStore() {
        override def dispose(): Unit = {} // prevent dispose from deleting our data
      }
      withCommand(ds) { command =>
        command.execute() must throwA[ParameterException](".*specify at least one.*") // no attributes to update
        command.params.attributes = Collections.singletonList("foo" -> "bar")
        command.execute() must throwA[ParameterException](".* exist .*") // schema doesn't exist
        ds.createSchema(sft)
        command.execute() must throwA[ParameterException](".*do not exist.*") // attributes are not in the schema
      }
    }

    "update attributes" in {
      val ds = new MemoryDataStore() {
        override def dispose(): Unit = {} // prevent dispose from deleting our data
      }
      ds.createSchema(sft)
      ds.addFeatures(features.map(ScalaSimpleFeature.copy): _*)
      withCommand(ds) { command =>
        command.params.attributes = Collections.singletonList("name" -> "bob")
        command.execute()
      }
      val updated = SelfClosingIterator(ds.getFeatureReader(new Query("tools"), Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
      updated.map(_.getID) mustEqual features.map(_.getID)
      foreach(Seq("level", "dtg", "geom")) { attribute =>
        updated.map(_.getAttribute(attribute)) mustEqual features.map(_.getAttribute(attribute))
      }
      updated.map(_.getAttribute("name")).toSet mustEqual Set("bob")
    }

    "update attributes with cql filter" in {
      val ds = new MemoryDataStore() {
        override def dispose(): Unit = {} // prevent dispose from deleting our data
      }
      ds.createSchema(sft)
      ds.addFeatures(features.map(ScalaSimpleFeature.copy): _*)
      withCommand(ds) { command =>
        command.params.attributes = Collections.singletonList("name" -> "bob")
        command.params.cqlFilter = ECQL.toFilter("dtg AFTER 2016-01-01T04:00:00.000Z")
        command.execute()
      }
      val updated = SelfClosingIterator(ds.getFeatureReader(new Query("tools"), Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
      updated.map(_.getID) mustEqual features.map(_.getID)
      foreach(Seq("level", "dtg", "geom")) { attribute =>
        updated.map(_.getAttribute(attribute)) mustEqual features.map(_.getAttribute(attribute))
      }
      updated.take(5).map(_.getAttribute("name")) mustEqual features.take(5).map(_.getAttribute("name"))
      updated.drop(5).map(_.getAttribute("name")).toSet mustEqual Set("bob")
    }
  }
}
