/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

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
  public void testTypeOf() throws Exception {
    try(RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        PointVector points = new PointVector("", allocator);
        LineStringVector lines = new LineStringVector("", allocator);
        MultiPointVector mpoints = new MultiPointVector("", allocator);
        MultiLineStringVector mlines = new MultiLineStringVector("", allocator);
        PolygonVector polys = new PolygonVector("", allocator);
        MultiPolygonVector mpolys = new MultiPolygonVector("", allocator)) {
      Assert.assertEquals(Point.class, GeometryVector.typeOf(points.getVector().getField()));
      Assert.assertEquals(LineString.class, GeometryVector.typeOf(lines.getVector().getField()));
      Assert.assertEquals(MultiPoint.class, GeometryVector.typeOf(mpoints.getVector().getField()));
      Assert.assertEquals(MultiLineString.class, GeometryVector.typeOf(mlines.getVector().getField()));
      Assert.assertEquals(Polygon.class, GeometryVector.typeOf(polys.getVector().getField()));
      Assert.assertEquals(MultiPolygon.class, GeometryVector.typeOf(mpolys.getVector().getField()));
    }
  }

  @Test
  public void testPoint() throws Exception {
    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();

    try(RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        PointVector vector = new PointVector("points", allocator)) {
      Field field = vector.getVector().getField();

      vector.getWriter().set(0, (Point) wktReader.read("POINT (0 20)"));
      vector.getWriter().set(1, (Point) wktReader.read("POINT (10 20)"));
      vector.getWriter().set(3, (Point) wktReader.read("POINT (30 20)"));
      vector.getWriter().setValueCount(4);

      Assert.assertEquals(4, vector.getReader().getValueCount());
      Assert.assertEquals(1, vector.getReader().getNullCount());
      Assert.assertEquals("POINT (0 20)", wktWriter.write(vector.getReader().get(0)));
      Assert.assertEquals("POINT (10 20)", wktWriter.write(vector.getReader().get(1)));
      Assert.assertEquals("POINT (30 20)", wktWriter.write(vector.getReader().get(3)));
      Assert.assertNull(vector.getReader().get(2));

      // ensure field was created correctly up front
      Assert.assertEquals(field, vector.getVector().getField());
      Assert.assertEquals(field.getChildren(), PointVector.fields);
    }
  }

  @Test
  public void testLineString() throws Exception {
    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();

    try(RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        LineStringVector vector = new LineStringVector("lines", allocator)) {
      Field field = vector.getVector().getField();

      vector.getWriter().set(0, (LineString) wktReader.read("LINESTRING (30 10, 10 30, 40 40)"));
      vector.getWriter().set(1, (LineString) wktReader.read("LINESTRING (40 10, 10 30)"));
      vector.getWriter().set(3, (LineString) wktReader.read("LINESTRING (30 10, 10 30, 40 45)"));
      vector.getWriter().setValueCount(4);

      Assert.assertEquals(4, vector.getReader().getValueCount());
      Assert.assertEquals(1, vector.getReader().getNullCount());
      Assert.assertEquals("LINESTRING (30 10, 10 30, 40 40)", wktWriter.write(vector.getReader().get(0)));
      Assert.assertEquals("LINESTRING (40 10, 10 30)", wktWriter.write(vector.getReader().get(1)));
      Assert.assertEquals("LINESTRING (30 10, 10 30, 40 45)", wktWriter.write(vector.getReader().get(3)));
      Assert.assertNull(vector.getReader().get(2));

      // ensure field was created correctly up front
      Assert.assertEquals(field, vector.getVector().getField());
      Assert.assertEquals(field.getChildren(), LineStringVector.fields);
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
        PolygonVector vector = new PolygonVector("polys", allocator)) {
      Field field = vector.getVector().getField();

      vector.getWriter().set(0, (Polygon) wktReader.read(p0));
      vector.getWriter().set(1, (Polygon) wktReader.read(p1));
      vector.getWriter().set(3, (Polygon) wktReader.read(p2));
      vector.getWriter().setValueCount(4);

      Assert.assertEquals(4, vector.getReader().getValueCount());
      Assert.assertEquals(1, vector.getReader().getNullCount());
      Assert.assertEquals(p0, wktWriter.write(vector.getReader().get(0)));
      Assert.assertEquals(p1, wktWriter.write(vector.getReader().get(1)));
      Assert.assertEquals(p2, wktWriter.write(vector.getReader().get(3)));
      Assert.assertNull(vector.getReader().get(2));

      // ensure field was created correctly up front
      Assert.assertEquals(field, vector.getVector().getField());
      Assert.assertEquals(field.getChildren(), PolygonVector.fields);
    }
  }

  @Test
  public void testMultiLineString() throws Exception {
    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();

    String mls0 = "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))";
    String mls1 = "MULTILINESTRING ((10 10, 20 30, 10 40), (30 40, 30 30, 40 20, 20 10))";
    String mls2 = "MULTILINESTRING ((10 10, 20 40, 10 40))";
    try(RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        MultiLineStringVector vector = new MultiLineStringVector("lines", allocator)) {
      Field field = vector.getVector().getField();

      vector.getWriter().set(0, (MultiLineString) wktReader.read(mls0));
      vector.getWriter().set(1, (MultiLineString) wktReader.read(mls1));
      vector.getWriter().set(3, (MultiLineString) wktReader.read(mls2));
      vector.getWriter().setValueCount(4);

      Assert.assertEquals(4, vector.getReader().getValueCount());
      Assert.assertEquals(1, vector.getReader().getNullCount());
      Assert.assertEquals(mls0, wktWriter.write(vector.getReader().get(0)));
      Assert.assertEquals(mls1, wktWriter.write(vector.getReader().get(1)));
      Assert.assertEquals(mls2, wktWriter.write(vector.getReader().get(3)));
      Assert.assertNull(vector.getReader().get(2));

      // ensure field was created correctly up front
      Assert.assertEquals(field, vector.getVector().getField());
      Assert.assertEquals(field.getChildren(), MultiLineStringVector.fields);
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
         MultiPointVector vector = new MultiPointVector("multipoints", allocator)) {
      Field field = vector.getVector().getField();

      vector.getWriter().set(0, (MultiPoint) wktReader.read(p0));
      vector.getWriter().set(1, (MultiPoint) wktReader.read(p1));
      vector.getWriter().set(3, (MultiPoint) wktReader.read(p2));
      vector.getWriter().setValueCount(4);

      Assert.assertEquals(4, vector.getReader().getValueCount());
      Assert.assertEquals(1, vector.getReader().getNullCount());
      Assert.assertEquals(p0, wktWriter.write(vector.getReader().get(0)));
      Assert.assertEquals(p1, wktWriter.write(vector.getReader().get(1)));
      Assert.assertEquals(p2, wktWriter.write(vector.getReader().get(3)));
      Assert.assertNull(vector.getReader().get(2));

      // ensure field was created correctly up front
      Assert.assertEquals(field, vector.getVector().getField());
      Assert.assertEquals(field.getChildren(), MultiPointVector.fields);
    }
  }

  @Test
  public void testMultiPolygon() throws Exception {
    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();

    String p0 = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))";
    String p1 = "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))";
    String p2 = "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)))";
    try(RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        MultiPolygonVector vector = new MultiPolygonVector("multipolys", allocator)) {
      Field field = vector.getVector().getField();

      vector.getWriter().set(0, (MultiPolygon) wktReader.read(p0));
      vector.getWriter().set(1, (MultiPolygon) wktReader.read(p1));
      vector.getWriter().set(3, (MultiPolygon) wktReader.read(p2));
      vector.getWriter().setValueCount(4);

      Assert.assertEquals(4, vector.getReader().getValueCount());
      Assert.assertEquals(1, vector.getReader().getNullCount());
      Assert.assertEquals(p0, wktWriter.write(vector.getReader().get(0)));
      Assert.assertEquals(p1, wktWriter.write(vector.getReader().get(1)));
      Assert.assertEquals(p2, wktWriter.write(vector.getReader().get(3)));
      Assert.assertNull(vector.getReader().get(2));

      // ensure field was created correctly up front
      Assert.assertEquals(field, vector.getVector().getField());
      Assert.assertEquals(field.getChildren(), MultiPolygonVector.fields);
    }
  }
}
