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
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.arrow.memory.RootAllocator;
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
        PolygonVector polys = new PolygonVector("", allocator)) {
      Assert.assertEquals(Point.class, GeometryVector.typeOf(points.getVector().getField()));
      Assert.assertEquals(LineString.class, GeometryVector.typeOf(lines.getVector().getField()));
      Assert.assertEquals(MultiPoint.class, GeometryVector.typeOf(mpoints.getVector().getField()));
      Assert.assertEquals(MultiLineString.class, GeometryVector.typeOf(mlines.getVector().getField()));
      Assert.assertEquals(Polygon.class, GeometryVector.typeOf(polys.getVector().getField()));
    }
  }
}
