/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{DataStore, DataStoreFinder}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data.DefaultTransaction
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.gt.partition.postgis.dialect.TableConfig
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.slf4j.LoggerFactory
import org.specs2.mutable.SpecificationWithJUnit
import org.testcontainers.containers.output.Slf4jLogConsumer

import java.sql.Connection
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

abstract class TestWithSharedPostgisContainer extends SpecificationWithJUnit with LazyLogging {

  import scala.collection.JavaConverters._

  val schema = "public"

  lazy val params = Map(
    "dbtype" -> PartitionedPostgisDataStoreParams.DbType.sample.toString,
    "host" -> TestWithSharedPostgisContainer.container.getHost,
    "port" -> TestWithSharedPostgisContainer.container.getFirstMappedPort.toString,
    "database" -> "postgres",
    "user" -> "postgres",
    "passwd" -> TestWithSharedPostgisContainer.container.password,
    "Batch insert size" -> "10",
  )

  val hours = 1
  val spec =
    "name:List[String],props:String:json=true,age:Int,dtg:Date,*geom:Point:srid=4326;" +
      Seq(
        s"pg.partitions.interval.hours=$hours",
        "pg.partitions.cron.minute=0"
      ).mkString(",")

  lazy val now = System.currentTimeMillis()

  def datastore(params: Map[String, _] = params): PartitionedPostgisDataStore = {
    val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[PartitionedPostgisDataStore]
    ds must not(beNull)
    ds
  }

  def sft(typeName: String, spec: String = spec): SimpleFeatureType = {
    if (!TestWithSharedPostgisContainer.typeNames.add(typeName)) {
      throw new IllegalArgumentException(s"Type name $typeName is already used by another test")
    }
    SimpleFeatureTypes.createType(typeName, spec)
  }

  def testSchema[T](ds: DataStore, typeName: String, spec: String = spec)(fn: SimpleFeatureType => T): T = {
    val sft = this.sft(typeName, spec)
    ds.createSchema(sft)
    try {
      val schema = ds.getSchema(typeName)
      schema must not(beNull)
      schema.getUserData.asScala must containAllOf(sft.getUserData.asScala.toSeq)
      logger.debug(s"Schema: ${SimpleFeatureTypes.encodeType(sft)}")
      fn(schema)
    } finally {
      ds.removeSchema(typeName) // clean up the schema to stop all the cron jobs running
    }
  }

  def features(sft: SimpleFeatureType): Seq[SimpleFeature] = Seq.tabulate(10) { i =>
    val builder = new SimpleFeatureBuilder(sft)
    builder.set("name", java.util.List.of(s"name$i", s"alt$i"))
    builder.set("age", i)
    builder.set("props", s"""{"age": $i, "names": ["name$i"]}""") // note: formatted so that the compare with sql works
    builder.set("dtg", new java.util.Date(now - ((i + 1) * 20 * 60 * 1000))) // 20 minutes
    builder.set("geom", WKTUtils.read(s"POINT(0 $i)"))
    val sf = builder.buildFeature(s"fid$i")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }

  def writeFeatures(ds: DataStore, sft: SimpleFeatureType): Seq[SimpleFeature] = {
    val features = this.features(sft)
    WithClose(new DefaultTransaction()) { tx =>
      WithClose(ds.getFeatureWriterAppend(sft.getTypeName, tx)) { writer =>
        features.foreach { feature =>
          FeatureUtils.write(writer, feature, useProvidedFid = true)
        }
      }
      tx.commit()
    }
    features
  }

  def compFromDb(sf: SimpleFeature): Seq[Any] = {
    Seq(sf.getID) ++ sf.getAttributes.asScala.map {
      // even though Timestamp extends Date, equals comparison doesn't work between the 2
      case t: java.sql.Timestamp => new java.util.Date(t.getTime)
      case a => a
    }
  }

  // note: jdbc data store adds the type name into the fid, so we add it here for the comparison
  def compWithFid(sf: SimpleFeature, sft: SimpleFeatureType, transforms: Option[Array[String]] = None): Seq[Any] =
    Seq(s"${sft.getTypeName}.${sf.getID}") ++
      transforms.getOrElse(sft.getAttributeDescriptors.asScala.map(_.getLocalName).toArray).map(sf.getAttribute)

  def count(cx: Connection, table: TableConfig): Int = {
    WithClose(cx.prepareStatement(s"select count(*) from ${table.name.qualified};")) { statement =>
      WithClose(statement.executeQuery()) { rs =>
        rs.next() must beTrue
        rs.getInt(1)
      }
    }
  }
}

object TestWithSharedPostgisContainer extends LazyLogging {

  lazy val container: PostgisContainer = {
    val container = new PostgisContainer()
    sys.addShutdownHook(container.close())
    if (logger.underlying.isDebugEnabled()) {
      container.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("postgis")))
    }
    if (logger.underlying.isTraceEnabled()) {
      container.withLogAllStatements()
    }
    container.start()
    container
  }

  private val typeNames = Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]())
}
