/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Assert;
import org.junit.Test;

public class GeometryVectorTest {

  @Test
  public void testPoint() throws Exception {
    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();

    String point1 = "POINT (0 20)";
    String point2 = "POINT (10 20)";
    String point3 = "POINT (30 20)";

    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         PointFloatVector floats = new PointFloatVector("points", allocator, null);
         PointVector doubles = new PointVector("points", allocator, null)) {

      Field floatField = floats.getVector().getField();
      Field doubleField = doubles.getVector().getField();

      floats.getWriter().set(0, (Point) wktReader.read(point1));
      floats.getWriter().set(1, (Point) wktReader.read(point2));
      floats.getWriter().set(3, (Point) wktReader.read(point3));
      floats.getWriter().setValueCount(4);

      doubles.getWriter().set(0, (Point) wktReader.read(point1));
      doubles.getWriter().set(1, (Point) wktReader.read(point2));
      doubles.getWriter().set(3, (Point) wktReader.read(point3));
      doubles.getWriter().setValueCount(4);

      Assert.assertEquals(4, floats.getReader().getValueCount());
      Assert.assertEquals(1, floats.getReader().getNullCount());
      Assert.assertEquals(point1, wktWriter.write(floats.getReader().get(0)));
      Assert.assertEquals(point2, wktWriter.write(floats.getReader().get(1)));
      Assert.assertEquals(point3, wktWriter.write(floats.getReader().get(3)));
      Assert.assertNull(floats.getReader().get(2));

      Assert.assertEquals(4, doubles.getReader().getValueCount());
      Assert.assertEquals(1, doubles.getReader().getNullCount());
      Assert.assertEquals(point1, wktWriter.write(doubles.getReader().get(0)));
      Assert.assertEquals(point2, wktWriter.write(doubles.getReader().get(1)));
      Assert.assertEquals(point3, wktWriter.write(doubles.getReader().get(3)));
      Assert.assertNull(doubles.getReader().get(2));

      // ensure field was created correctly up front

      Assert.assertEquals(floatField, floats.getVector().getField());
      Assert.assertEquals(floatField.getChildren(), PointFloatVector.fields);

      Assert.assertEquals(doubleField, doubles.getVector().getField());
      Assert.assertEquals(doubleField.getChildren(), PointVector.fields);
    }
  }

  @Test
  public void testLineString() throws Exception {
    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();

    String line1 = "LINESTRING (30 10, 10 30, 40 40)";
    String line2 = "LINESTRING (40 10, 10 30)";
    String line3 = "LINESTRING (30 10, 10 30, 40 45, 55 60, 56 60)";

    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         LineStringFloatVector floats = new LineStringFloatVector("lines", allocator, null);
         LineStringVector doubles = new LineStringVector("lines", allocator, null)) {

      Field floatField = floats.getVector().getField();
      Field doubleField = doubles.getVector().getField();

      floats.getWriter().set(0, (LineString) wktReader.read(line1));
      floats.getWriter().set(1, (LineString) wktReader.read(line2));
      floats.getWriter().set(3, (LineString) wktReader.read(line3));
      floats.getWriter().setValueCount(4);

      doubles.getWriter().set(0, (LineString) wktReader.read(line1));
      doubles.getWriter().set(1, (LineString) wktReader.read(line2));
      doubles.getWriter().set(3, (LineString) wktReader.read(line3));
      doubles.getWriter().setValueCount(4);

      Assert.assertEquals(4, floats.getReader().getValueCount());
      Assert.assertEquals(1, floats.getReader().getNullCount());
      Assert.assertEquals(line1, wktWriter.write(floats.getReader().get(0)));
      Assert.assertEquals(line2, wktWriter.write(floats.getReader().get(1)));
      Assert.assertEquals(line3, wktWriter.write(floats.getReader().get(3)));
      Assert.assertNull(floats.getReader().get(2));

      Assert.assertEquals(4, doubles.getReader().getValueCount());
      Assert.assertEquals(1, doubles.getReader().getNullCount());
      Assert.assertEquals(line1, wktWriter.write(doubles.getReader().get(0)));
      Assert.assertEquals(line2, wktWriter.write(doubles.getReader().get(1)));
      Assert.assertEquals(line3, wktWriter.write(doubles.getReader().get(3)));
      Assert.assertNull(doubles.getReader().get(2));

      // ensure field was created correctly up front

      Assert.assertEquals(floatField, floats.getVector().getField());
      Assert.assertEquals(floatField.getChildren(), LineStringFloatVector.fields);

      Assert.assertEquals(doubleField, doubles.getVector().getField());
      Assert.assertEquals(doubleField.getChildren(), LineStringVector.fields);
    }
  }

  @Test
  public void testPolygon() throws Exception {
    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();

    String p0 = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
    String p1 = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))";
    String p2 = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30), (19 36, 23 38, 22 34, 19 36))";
    try(RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        PolygonFloatVector floats = new PolygonFloatVector("polys", allocator, null);
        PolygonVector doubles = new PolygonVector("polys", allocator, null)) {

      Field floatField = floats.getVector().getField();
      Field doubleField = doubles.getVector().getField();

      floats.getWriter().set(0, (Polygon) wktReader.read(p0));
      floats.getWriter().set(1, (Polygon) wktReader.read(p1));
      floats.getWriter().set(3, (Polygon) wktReader.read(p2));
      floats.getWriter().setValueCount(4);

      doubles.getWriter().set(0, (Polygon) wktReader.read(p0));
      doubles.getWriter().set(1, (Polygon) wktReader.read(p1));
      doubles.getWriter().set(3, (Polygon) wktReader.read(p2));
      doubles.getWriter().setValueCount(4);

      Assert.assertEquals(4, floats.getReader().getValueCount());
      Assert.assertEquals(1, floats.getReader().getNullCount());
      Assert.assertEquals(p0, wktWriter.write(floats.getReader().get(0)));
      Assert.assertEquals(p1, wktWriter.write(floats.getReader().get(1)));
      Assert.assertEquals(p2, wktWriter.write(floats.getReader().get(3)));
      Assert.assertNull(floats.getReader().get(2));

      Assert.assertEquals(4, doubles.getReader().getValueCount());
      Assert.assertEquals(1, doubles.getReader().getNullCount());
      Assert.assertEquals(p0, wktWriter.write(doubles.getReader().get(0)));
      Assert.assertEquals(p1, wktWriter.write(doubles.getReader().get(1)));
      Assert.assertEquals(p2, wktWriter.write(doubles.getReader().get(3)));
      Assert.assertNull(doubles.getReader().get(2));

      // ensure field was created correctly up front

      Assert.assertEquals(floatField, floats.getVector().getField());
      Assert.assertEquals(floatField.getChildren(), PolygonFloatVector.fields);

      Assert.assertEquals(doubleField, doubles.getVector().getField());
      Assert.assertEquals(doubleField.getChildren(), PolygonVector.fields);
    }
  }

  @Test
  public void testMultiLineString() throws Exception {
    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();

    String mls0 = "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))";
    String mls1 = "MULTILINESTRING ((10 10, 20 30, 10 40), (30 40, 30 30, 40 20, 20 10), (40 50, 40 40, 50 30, 30 20))";
    String mls2 = "MULTILINESTRING ((10 10, 20 40, 10 40))";
    try(RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        MultiLineStringFloatVector floats = new MultiLineStringFloatVector("lines", allocator, null);
        MultiLineStringVector doubles = new MultiLineStringVector("lines", allocator, null)) {

      Field floatField = floats.getVector().getField();
      Field doubleField = doubles.getVector().getField();

      floats.getWriter().set(0, (MultiLineString) wktReader.read(mls0));
      floats.getWriter().set(1, (MultiLineString) wktReader.read(mls1));
      floats.getWriter().set(3, (MultiLineString) wktReader.read(mls2));
      floats.getWriter().setValueCount(4);

      doubles.getWriter().set(0, (MultiLineString) wktReader.read(mls0));
      doubles.getWriter().set(1, (MultiLineString) wktReader.read(mls1));
      doubles.getWriter().set(3, (MultiLineString) wktReader.read(mls2));
      doubles.getWriter().setValueCount(4);

      Assert.assertEquals(4, floats.getReader().getValueCount());
      Assert.assertEquals(1, floats.getReader().getNullCount());
      Assert.assertEquals(mls0, wktWriter.write(floats.getReader().get(0)));
      Assert.assertEquals(mls1, wktWriter.write(floats.getReader().get(1)));
      Assert.assertEquals(mls2, wktWriter.write(floats.getReader().get(3)));
      Assert.assertNull(floats.getReader().get(2));

      Assert.assertEquals(4, doubles.getReader().getValueCount());
      Assert.assertEquals(1, doubles.getReader().getNullCount());
      Assert.assertEquals(mls0, wktWriter.write(doubles.getReader().get(0)));
      Assert.assertEquals(mls1, wktWriter.write(doubles.getReader().get(1)));
      Assert.assertEquals(mls2, wktWriter.write(doubles.getReader().get(3)));
      Assert.assertNull(doubles.getReader().get(2));

      // ensure field was created correctly up front

      Assert.assertEquals(doubleField, doubles.getVector().getField());
      Assert.assertEquals(doubleField.getChildren(), MultiLineStringVector.fields);

      Assert.assertEquals(floatField, floats.getVector().getField());
      Assert.assertEquals(floatField.getChildren(), MultiLineStringFloatVector.fields);
    }
  }

  @Test
  public void testMultiPoint() throws Exception {
    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();

    String p0 = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))";
    String p1 = "MULTIPOINT ((10 40))";
    String p2 = "MULTIPOINT ((40 30), (20 20))";
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         MultiPointFloatVector floats = new MultiPointFloatVector("multipoints", allocator, null);
         MultiPointVector doubles = new MultiPointVector("multipoints", allocator, null)) {

      Field floatField = floats.getVector().getField();
      Field doubleField = doubles.getVector().getField();

      floats.getWriter().set(0, (MultiPoint) wktReader.read(p0));
      floats.getWriter().set(1, (MultiPoint) wktReader.read(p1));
      floats.getWriter().set(3, (MultiPoint) wktReader.read(p2));
      floats.getWriter().setValueCount(4);

      doubles.getWriter().set(0, (MultiPoint) wktReader.read(p0));
      doubles.getWriter().set(1, (MultiPoint) wktReader.read(p1));
      doubles.getWriter().set(3, (MultiPoint) wktReader.read(p2));
      doubles.getWriter().setValueCount(4);

      Assert.assertEquals(4, floats.getReader().getValueCount());
      Assert.assertEquals(1, floats.getReader().getNullCount());
      Assert.assertEquals(p0, wktWriter.write(floats.getReader().get(0)));
      Assert.assertEquals(p1, wktWriter.write(floats.getReader().get(1)));
      Assert.assertEquals(p2, wktWriter.write(floats.getReader().get(3)));
      Assert.assertNull(floats.getReader().get(2));

      Assert.assertEquals(4, doubles.getReader().getValueCount());
      Assert.assertEquals(1, doubles.getReader().getNullCount());
      Assert.assertEquals(p0, wktWriter.write(doubles.getReader().get(0)));
      Assert.assertEquals(p1, wktWriter.write(doubles.getReader().get(1)));
      Assert.assertEquals(p2, wktWriter.write(doubles.getReader().get(3)));
      Assert.assertNull(doubles.getReader().get(2));

      // ensure field was created correctly up front

      Assert.assertEquals(floatField, floats.getVector().getField());
      Assert.assertEquals(floatField.getChildren(), MultiPointFloatVector.fields);

      Assert.assertEquals(doubleField, doubles.getVector().getField());
      Assert.assertEquals(doubleField.getChildren(), MultiPointVector.fields);
    }
  }

  @Test
  public void testMultiPolygon() throws Exception {
    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();

    String p0 = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))";
    String p1 = "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))";
    String p2 = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)))";

    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         MultiPolygonFloatVector floats = new MultiPolygonFloatVector("multipolys", allocator, null);
         MultiPolygonVector doubles = new MultiPolygonVector("multipolys", allocator, null)) {

      Field floatField = floats.getVector().getField();
      Field doubleField = doubles.getVector().getField();

      floats.getWriter().set(0, (MultiPolygon) wktReader.read(p0));
      floats.getWriter().set(1, (MultiPolygon) wktReader.read(p1));
      floats.getWriter().set(3, (MultiPolygon) wktReader.read(p2));
      floats.getWriter().setValueCount(4);

      doubles.getWriter().set(0, (MultiPolygon) wktReader.read(p0));
      doubles.getWriter().set(1, (MultiPolygon) wktReader.read(p1));
      doubles.getWriter().set(3, (MultiPolygon) wktReader.read(p2));
      doubles.getWriter().setValueCount(4);

      Assert.assertEquals(4, floats.getReader().getValueCount());
      Assert.assertEquals(1, floats.getReader().getNullCount());
      Assert.assertEquals(p0, wktWriter.write(floats.getReader().get(0)));
      Assert.assertEquals(p1, wktWriter.write(floats.getReader().get(1)));
      Assert.assertEquals(p2, wktWriter.write(floats.getReader().get(3)));
      Assert.assertNull(floats.getReader().get(2));

      Assert.assertEquals(4, doubles.getReader().getValueCount());
      Assert.assertEquals(1, doubles.getReader().getNullCount());
      Assert.assertEquals(p0, wktWriter.write(doubles.getReader().get(0)));
      Assert.assertEquals(p1, wktWriter.write(doubles.getReader().get(1)));
      Assert.assertEquals(p2, wktWriter.write(doubles.getReader().get(3)));
      Assert.assertNull(doubles.getReader().get(2));

      // ensure field was created correctly up front

      Assert.assertEquals(floatField, floats.getVector().getField());
      Assert.assertEquals(floatField.getChildren(), MultiPolygonFloatVector.fields);

      Assert.assertEquals(doubleField, doubles.getVector().getField());
      Assert.assertEquals(doubleField.getChildren(), MultiPolygonVector.fields);
    }
  }
}
