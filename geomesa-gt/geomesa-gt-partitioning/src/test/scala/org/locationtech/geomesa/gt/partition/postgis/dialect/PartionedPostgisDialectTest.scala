/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis.dialect

import org.geotools.feature.AttributeTypeBuilder
import org.geotools.jdbc.JDBCDataStore
import org.junit.runner.RunWith
import org.locationtech.jts.geom.Point
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PartionedPostgisDialectTest extends Specification {

  "PartitionedPostgisDialect" should {

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
}
