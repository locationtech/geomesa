/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.LineString;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.reader.LineStringReader;
import org.locationtech.geomesa.arrow.vector.writer.LineStringWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LineStringVector implements GeometryVector<LineString> {

  private static final String X_FIELD = "x";
  private static final String Y_FIELD = "y";

  static final List<Field> fields =
    Collections.unmodifiableList(new ArrayList<>(Arrays.asList(
      new Field(X_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_FIELD),
      new Field(Y_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_FIELD)
    )));

  private final NullableMapVector vector;
  private final LineStringWriter writer;
  private final LineStringReader reader;

  public LineStringVector(String name, BufferAllocator allocator) {
    this(new NullableMapVector(name, allocator, null, null));
    this.vector.allocateNew();
  }

  public LineStringVector(NullableMapVector vector) {
    this.vector = vector;
    // create the fields we will write to up front
    // they will be automatically created at write, but we want the field pre-defined
    vector.addOrGet(X_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.FLOAT8, null);
    vector.addOrGet(Y_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.FLOAT8, null);
    this.writer = new LineStringWriter(new NullableMapWriter(vector), X_FIELD, Y_FIELD);
    this.reader = new LineStringReader(vector, X_FIELD, Y_FIELD);
  }

  @Override
  public LineStringWriter getWriter() {
    return writer;
  }

  @Override
  public LineStringReader getReader() {
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
