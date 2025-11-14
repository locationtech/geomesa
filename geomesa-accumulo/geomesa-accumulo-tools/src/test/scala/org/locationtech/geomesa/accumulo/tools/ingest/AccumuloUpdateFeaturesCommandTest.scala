/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import org.apache.accumulo.core.security.Authorizations
import org.geomesa.testcontainers.AccumuloContainer
import org.geotools.api.data.{Query, Transaction}
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloRunner}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.util.concurrent.atomic.AtomicInteger

class AccumuloUpdateFeaturesCommandTest extends SpecificationWithJUnit {

  private val sftCounter = new AtomicInteger(0)

  val sft = SimpleFeatureTypes.createType("tools", "name:String,level:String,dtg:Date,*geom:Point:srid=4326")
  val features = Seq.tabulate(10) { i =>
    val sf = ScalaSimpleFeature.create(sft, s"id$i", s"name$i", "user", s"2016-01-01T0$i:00:00.000Z", "POINT(1 0)")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "user")
    sf
  }

  def baseArgs: Array[String] = Array(
    "update-features",
    "--force",
    "--instance",      AccumuloContainer.getInstance().getInstanceName,
    "--zookeepers",    AccumuloContainer.getInstance().getZookeepers,
    "--user",          AccumuloContainer.getInstance().getUsername,
    "--password",      AccumuloContainer.getInstance().getPassword,
    "--auths",         "user,admin",
    "--catalog",       s"gm.${getClass.getSimpleName}${sftCounter.getAndIncrement()}",
    "--feature-name",  sft.getTypeName,
  )

  def execute(args: Array[String]): List[SimpleFeature] = {
    val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
    command.withDataStore { ds =>
      ds.createSchema(sft)
      ds.getFeatureSource(sft.getTypeName).addFeatures(new ListFeatureCollection(sft, features: _*))
    }
    command.execute()
    command.withDataStore { ds =>
      try {
        SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toList.sortBy(_.getID)
      } finally {
        ds.delete()
      }
    }
  }

  step {
    WithClose(AccumuloContainer.getInstance().client()) { client =>
      client.securityOperations()
        .changeUserAuthorizations(AccumuloContainer.getInstance().getUsername, new Authorizations("user", "admin"))
    }
  }

  "AccumuloUpdateFeaturesCommand" should {
    "update attributes" in {
      val updated = execute(baseArgs ++ Array("--set", "name=bob"))
      updated.map(_.getID) mustEqual features.map(_.getID)
      foreach(Seq("level", "dtg", "geom")) { attribute =>
        updated.map(_.getAttribute(attribute)) mustEqual features.map(_.getAttribute(attribute))
      }
      updated.map(_.getAttribute("name")).toSet mustEqual Set("bob")
    }

    "update attributes with cql filter" in {
      val updated = execute(baseArgs ++ Array("--set", "name=bob", "--cql", "dtg AFTER 2016-01-01T04:00:00.000Z"))
      updated.map(_.getID) mustEqual features.map(_.getID)
      foreach(Seq("level", "dtg", "geom")) { attribute =>
        updated.map(_.getAttribute(attribute)) mustEqual features.map(_.getAttribute(attribute))
      }
      updated.take(5).map(_.getAttribute("name")) mustEqual features.take(5).map(_.getAttribute("name"))
      updated.drop(5).map(_.getAttribute("name")).toSet mustEqual Set("bob")
    }

    "update visibility with cql filter" in {
      val updated =
        execute(baseArgs ++ Array("--set", "level=admin", "--set-visibility", "admin", "--cql", "dtg AFTER 2016-01-01T04:00:00.000Z"))
      updated.map(_.getID) mustEqual features.map(_.getID)
      foreach(Seq("name", "dtg", "geom")) { attribute =>
        updated.map(_.getAttribute(attribute)) mustEqual features.map(_.getAttribute(attribute))
      }
      updated.take(5).map(_.getAttribute("level")).toSet mustEqual Set("user")
      updated.take(5).map(_.getUserData.get(SecurityUtils.FEATURE_VISIBILITY)).toSet mustEqual Set("user")
      updated.drop(5).map(_.getAttribute("level")).toSet mustEqual Set("admin")
      updated.drop(5).map(_.getUserData.get(SecurityUtils.FEATURE_VISIBILITY)).toSet mustEqual Set("admin")
    }
  }
}
