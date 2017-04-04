/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Assert;
import org.junit.Test;

public class MultiPolygonVectorTest {

  @Test
  public void testSetGet() throws Exception {
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
