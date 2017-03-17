/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector.writer;

import com.vividsolutions.jts.geom.Point;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.Float8Writer;

public class PointWriter implements GeometryWriter<Point> {

  private final MapWriter writer;
  private final Float8Writer xWriter;
  private final Float8Writer yWriter;

  public PointWriter(MapWriter writer) {
    this.writer = writer;
    this.xWriter = writer.float8("x");
    this.yWriter = writer.float8("y");
  }

  @Override
  public void set(Point geom) {
    if (geom != null) {
      writer.start();
      xWriter.writeFloat8(geom.getX());
      yWriter.writeFloat8(geom.getY());
      writer.end();
    }
  }

  @Override
  public void set(int i, Point geom) {
    writer.setPosition(i);
    set(geom);
  }

  @Override
  public void setValueCount(int count) {
    if (writer instanceof NullableMapWriter) {
      ((NullableMapWriter) writer).setValueCount(count);
    } else {
      throw new RuntimeException("Not nullable writer: " + writer);
    }
  }

  @Override
  public void close() throws Exception {
    writer.close();
  }
}
