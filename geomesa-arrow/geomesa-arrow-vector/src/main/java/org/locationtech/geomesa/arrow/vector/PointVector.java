/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Point;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.locationtech.geomesa.arrow.vector.reader.PointReader;
import org.locationtech.geomesa.arrow.vector.writer.PointWriter;

public class PointVector implements GeometryVector<Point> {

  private final NullableMapVector vector;
  private final PointWriter writer;
  private final PointReader reader;

  public PointVector(String name, BufferAllocator allocator) {
    this(new NullableMapVector(name, allocator, null, null));
  }

  public PointVector(NullableMapVector vector) {
    this.vector = vector;
    vector.addOrGet("x", MinorType.FLOAT8, NullableFloat8Vector.class, null);
    vector.addOrGet("y", MinorType.FLOAT8, NullableFloat8Vector.class, null);
    this.writer = new PointWriter(vector);
    this.reader = new PointReader(vector);
  }

  @Override
  public PointWriter getWriter() {
    return writer;
  }

  @Override
  public PointReader getReader() {
    return reader;
  }

  @Override
  public NullableMapVector getVector() {
    return vector;
  }

  @Override
  public void close() throws Exception {
    writer.close();
    reader.close();
    vector.close();
  }
}
