/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.jdbc

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Point
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.DockerImageName

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager}
import java.util.Date

@RunWith(classOf[JUnitRunner])
class JdbcConverterTest extends Specification with BeforeAfterAll with LazyLogging {

  sequential

  var connection: Connection = _

  val sft = SimpleFeatureTypes.createType("example", "name:String,dtg:Date,*geom:Point:srid=4326")
  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", s"2017-02-03T00:0$i:01.000Z", s"POINT(40 6$i)")
  }

  var container: PostgreSQLContainer[_] = _

  lazy val url = s"${container.getJdbcUrl}&user=${container.getUsername}&password=${container.getPassword}"

  override def beforeAll(): Unit = {
    val image = DockerImageName.parse("postgres").withTag(sys.props.getOrElse("postgres.docker.tag", "15.1"))
    container = new PostgreSQLContainer(image)
    // if we don't set the default db/name to postgres, the startup check fails as it restarts 3 times instead of the expected 2
    container.withDatabaseName("postgres")
    container.withUsername("postgres")
    container.start()
    container.followOutput(new Slf4jLogConsumer(logger.underlying))

    val create = "create table example(id BIGINT NOT NULL PRIMARY KEY, name VARCHAR, dtg TIMESTAMP, lat DOUBLE PRECISION, lon DOUBLE PRECISION);"
    val insert = "INSERT INTO example(id, name, dtg, lat, lon) VALUES (?, ?, ?, ?, ?);"

    WithClose(DriverManager.getConnection(url)) { connection =>
      WithClose(connection.prepareStatement(create))(_.execute)
      WithClose(connection.prepareStatement(insert)) { statement =>
        features.foreach { feature =>
          statement.setObject(1, feature.getID.toInt)
          statement.setObject(2, feature.getAttribute("name"))
          statement.setObject(3, new java.sql.Timestamp(feature.getAttribute("dtg").asInstanceOf[Date].getTime))
          statement.setObject(4, feature.getAttribute("geom").asInstanceOf[Point].getY)
          statement.setObject(5, feature.getAttribute("geom").asInstanceOf[Point].getX)
          statement.executeUpdate()
        }
      }
    }

  }

  override def afterAll(): Unit = {
    if (container != null) {
      container.stop()
    }
  }

  "JdbcConverter" should {
    "process select statements" >> {
      val conf = ConfigFactory.parseString(
        s"""
          | {
          |   type       = "jdbc",
          |   id-field   = "$$1::string",
          |   connection = "$url"
          |   fields = [
          |     { name = "name", transform = "$$2" },
          |     { name = "dtg",  transform = "$$3" },
          |     { name = "lat",  transform = "$$4" },
          |     { name = "lon",  transform = "$$5" },
          |     { name = "geom", transform = "point($$lon, $$lat)" }
          |   ]
          | }
        """.stripMargin)

      WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        converter must not(beNull)

        val sql = new ByteArrayInputStream("select * from example".getBytes(StandardCharsets.UTF_8))
        val res = WithClose(converter.process(sql))(_.toList)
        // note: comparison has to be done backwards,
        // as java.util.Date.equals(java.sql.Timestamp) != java.sql.Timestamp.equals(java.util.Date)
        features mustEqual res
      }
    }
  }
}
