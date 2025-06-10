/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data._
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.net.ServerSocket

@RunWith(classOf[JUnitRunner])
class PostgisResiliencyTest extends Specification with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  private val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  private val features = Seq.tabulate(2) { i =>
    ScalaSimpleFeature.create(sft, s"${sft.getTypeName}.$i", s"name$i", i, s"2025-06-01T0$i:00:00.000Z", s"POINT (0 $i)")
  }

  lazy val params = Map(
    "dbtype" -> PartitionedPostgisDataStoreParams.DbType.sample,
    "host" -> postgis.getHost,
    "port" -> port.toString,
    "database" -> "postgres",
    "user" -> "postgres",
    "passwd" -> postgis.password,
    "Batch insert size" -> "10",
  )

  private val port = {
    val socket = new ServerSocket(0)
    try { socket.getLocalPort } finally {
      socket.close()
    }
  }

  private val postgis = new PostgisContainer(Some(port))

  if (logger.underlying.isTraceEnabled()) {
    postgis.withLogAllStatements()
  }

  def write(ds: DataStore, features: Seq[SimpleFeature]): Unit = {
    WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
      features.foreach { f =>
        val toWrite = FeatureUtils.copyToWriter(writer, f, useProvidedFid = true)
        toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(toWrite.getID.substring(sft.getTypeName.length + 1))
        writer.write()
      }
    }
  }

  def query(ds: DataStore): List[SimpleFeature] =
    WithClose(CloseableIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)))(_.toList)

  "PartitionedPostgisDataStore" should {

    "fail to get a data store if postgres is down" in {
      DataStoreFinder.getDataStore(params.asJava) must throwAn[Exception]
    }

    "recover from a temporary database failure" in {
      postgis.start()
      try {
        WithClose(DataStoreFinder.getDataStore(params.asJava)) { ds =>
          ds must not(beNull)
          ds.getTypeNames must beEmpty
          ds.createSchema(sft)
          write(ds, features.take(1))
          postgis.stop()
          ds.getTypeNames must throwAn[Exception]
          postgis.start()
          ds.getTypeNames mustEqual Array(sft.getTypeName)
          features.take(1) mustEqual query(ds) // note: have to compare backwards for equality checks to work
          write(ds, features.drop(1))
          features mustEqual query(ds) // note: have to compare backwards for equality checks to work
        }
      } finally {
        postgis.close()
      }
    }
  }
}
