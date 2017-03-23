/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Test;

public class MultiLineStringVectorTest {

  @Test
  public void testSetGet() throws Exception {
    WKTReader wktReader = new WKTReader();
    WKTWriter wktWriter = new WKTWriter();

    String mls0 = "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))";
    String mls1 = "MULTILINESTRING ((10 10, 20 30, 10 40), (30 40, 30 30, 40 20, 20 10))";
    String mls2 = "MULTILINESTRING ((10 10, 20 40, 10 40))";
    try(RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        MultiLineStringVector vector = new MultiLineStringVector("lines", allocator)) {
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
    }
  }

}
