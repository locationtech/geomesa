/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Test;

public class PointVectorTest {

  @Test
  public void testSetGet() throws Exception {
    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();

    try(RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        PointVector vector = new PointVector("points", allocator)) {
      vector.getWriter().write(0, (Point) wktReader.read("POINT (0 20)"));
      vector.getWriter().write(1, (Point) wktReader.read("POINT (10 20)"));
      vector.getWriter().write(3, (Point) wktReader.read("POINT (30 20)"));
      vector.getWriter().setValueCount(4);

      Assert.assertEquals(4, vector.getReader().getValueCount());
      Assert.assertEquals(1, vector.getReader().getNullCount());
      Assert.assertEquals("POINT (0 20)", wktWriter.write(vector.getReader().read(0)));
      Assert.assertEquals("POINT (10 20)", wktWriter.write(vector.getReader().read(1)));
      Assert.assertEquals("POINT (30 20)", wktWriter.write(vector.getReader().read(3)));
      Assert.assertNull(vector.getReader().read(2));
    }
  }

}
