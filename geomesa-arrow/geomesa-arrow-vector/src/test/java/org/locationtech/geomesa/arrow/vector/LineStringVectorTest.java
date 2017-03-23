/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Test;

public class LineStringVectorTest {

  @Test
  public void testSetGet() throws Exception {
    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();

    try(RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        LineStringVector vector = new LineStringVector("lines", allocator)) {
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
    }
  }

}
