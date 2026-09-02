/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect

import org.geotools.feature.AttributeTypeBuilder
import org.geotools.jdbc.JDBCDataStore
import org.junit.runner.RunWith
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.VisCol
import org.locationtech.geomesa.gt.partition.postgis.dialect.tables.MainView
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Point
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.sql.Connection

@RunWith(classOf[JUnitRunner])
class PartionedPostgisDialectTest extends Specification with Mockito {

  "PartitionedPostgisDialect" should {

    "add a pg_vis predicate to each main view branch when a _vis column is present" in {
      val sft = SimpleFeatureTypes.createType("vistest", s"name:String,dtg:Date,*geom:Point:srid=4326,$VisCol:String")
      val info = TypeInfo("public", sft)
      info.cols.vis must beSome
      val sqlCapture = new SqlCapture()
      MainView.create(info)(sqlCapture)
      // one predicate per union branch (write ahead, wa partitions, main partitions, spill)
      sqlCapture.sql.split("pg_vis").length - 1 mustEqual 4
      sqlCapture.sql must contain(s"""pg_vis(${escape(VisCol)}, (SELECT string_to_array(current_setting('geomesa.auths', true), ',')))""")
    }

    "not add a pg_vis predicate when there is no _vis column" in {
      val sft = SimpleFeatureTypes.createType("novistest", "name:String,dtg:Date,*geom:Point:srid=4326")
      val info = TypeInfo("public", sft)
      info.cols.vis must beNone
      val sqlCapture = new SqlCapture()
      MainView.create(info)(sqlCapture)
      sqlCapture.sql must not(contain("pg_vis"))
    }

    "signal vis enabled/disabled via the pg.vis.enabled user data flag" in {
      import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect.SftUserData
      val disabled = SimpleFeatureTypes.createType("visdefault", "name:String,dtg:Date,*geom:Point:srid=4326")
      SftUserData.VisEnabled.get(disabled) must beFalse
      val enabled =
        SimpleFeatureTypes.createType("vison", "name:String,dtg:Date,*geom:Point:srid=4326;pg.vis.enabled='true'")
      SftUserData.VisEnabled.get(enabled) must beTrue
    }

    "Escape literal values" in {
      SqlLiteral("foo'bar").raw mustEqual "foo'bar"
      SqlLiteral("foo'bar").quoted mustEqual "'foo''bar'"
      SqlLiteral("foo\"bar").quoted mustEqual "'foo\"bar'"
    }

    "Escape identifiers" in {
      FunctionName("foo'bar").raw mustEqual "foo'bar"
      FunctionName("foo'bar").quoted mustEqual "\"foo'bar\""
      FunctionName("foo\"bar").quoted mustEqual "\"foo\"\"bar\""
    }

    "handle strings or ints as user data" in {
      foreach(Seq("4326", 4326, Int.box(4326), null)) { srid =>
        val builder = new AttributeTypeBuilder().binding(classOf[Point])
        builder.userData(JDBCDataStore.JDBC_NATIVE_SRID, srid)
        builder.crs(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)
        val attr = builder.buildDescriptor("geom")
        val buf = new StringBuffer("geometry")
        new PartitionedPostgisDialect(null).encodePostColumnCreateTable(attr, buf)
        buf.toString mustEqual "geometry (POINT, 4326)"
      }
    }
  }

  class SqlCapture extends ExecutionContext(mock[Connection]) {
    var sql: String = ""
    override def execute(sql: String): Unit = this.sql = sql
  }
}
