/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Point;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.reader.PointReader;
import org.locationtech.geomesa.arrow.vector.writer.PointWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PointVector implements GeometryVector<Point> {

  private static final String X_FIELD = "x";
  private static final String Y_FIELD = "y";

  static final List<Field> fields =
    Collections.unmodifiableList(new ArrayList<>(Arrays.asList(
      new Field(X_FIELD, true, ArrowHelper.DOUBLE_TYPE, null),
      new Field(Y_FIELD, true, ArrowHelper.DOUBLE_TYPE, null)
    )));

  private final NullableMapVector vector;
  private final PointWriter writer;
  private final PointReader reader;

  public PointVector(String name, BufferAllocator allocator) {
    this(new NullableMapVector(name, allocator, null, null));
    this.vector.allocateNew();
  }

  public PointVector(NullableMapVector vector) {
    this.vector = vector;
    // create the fields we will write to up front
    // they will be automatically created at write, but we want the field pre-defined
    vector.addOrGet(X_FIELD, MinorType.FLOAT8, NullableFloat8Vector.class, null);
    vector.addOrGet(Y_FIELD, MinorType.FLOAT8, NullableFloat8Vector.class, null);
    this.writer = new PointWriter(new NullableMapWriter(vector), X_FIELD, Y_FIELD);
    this.reader = new PointReader(vector, X_FIELD, Y_FIELD);
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
