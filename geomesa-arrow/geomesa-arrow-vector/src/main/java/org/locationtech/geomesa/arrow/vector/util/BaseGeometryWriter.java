/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector.util;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.locationtech.geomesa.arrow.vector.GeometryVector.GeometryWriter;

public abstract class BaseGeometryWriter<T extends Geometry> implements GeometryWriter<T> {

  private final MapWriter writer;

  public BaseGeometryWriter(MapWriter writer) {
    this.writer = writer;
  }

  protected abstract void writeGeometry(T geom);

  @Override
  public void set(T geom) {
    if (geom != null) {
      writer.start();
      writeGeometry(geom);
      writer.end();
    }
  }

  @Override
  public void set(int i, T geom) {
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
