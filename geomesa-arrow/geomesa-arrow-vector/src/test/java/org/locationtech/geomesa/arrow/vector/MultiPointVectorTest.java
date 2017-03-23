/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Assert;
import org.junit.Test;

public class MultiPointVectorTest {

  @Test
  public void testSetGet() throws Exception {
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

}
