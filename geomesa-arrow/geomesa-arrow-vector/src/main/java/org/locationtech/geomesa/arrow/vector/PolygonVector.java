/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Polygon;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.reader.PolygonReader;
import org.locationtech.geomesa.arrow.vector.writer.PolygonWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PolygonVector implements GeometryVector<Polygon> {

  private static final String X_EXTERIOR_FIELD = "x-ext";
  private static final String Y_EXTERIOR_FIELD = "y-ext";
  private static final String X_INTERIOR_FIELD = "x-int";
  private static final String Y_INTERIOR_FIELD = "y-int";

  static final List<Field> fields =
    Collections.unmodifiableList(new ArrayList<>(Arrays.asList(
      new Field(X_EXTERIOR_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_FIELD),
      new Field(Y_EXTERIOR_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_FIELD),
      new Field(X_INTERIOR_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.NESTED_DOUBLE_FIELD),
      new Field(Y_INTERIOR_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.NESTED_DOUBLE_FIELD)
    )));

  private final NullableMapVector vector;
  private final PolygonWriter writer;
  private final PolygonReader reader;

  public PolygonVector(String name, BufferAllocator allocator) {
    this(new NullableMapVector(name, allocator, null, null));
    this.vector.allocateNew();
  }

  public PolygonVector(NullableMapVector vector) {
    this.vector = vector;
    // create the fields we will write to up front
    // they will be automatically created at write, but we want the field pre-defined
    vector.addOrGet(X_EXTERIOR_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.FLOAT8, null);
    vector.addOrGet(Y_EXTERIOR_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.FLOAT8, null);
    ((ListVector)vector.addOrGet(X_INTERIOR_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.LIST, null).getVector()).addOrGetVector(MinorType.FLOAT8, null);
    ((ListVector)vector.addOrGet(Y_INTERIOR_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.LIST, null).getVector()).addOrGetVector(MinorType.FLOAT8, null);
    this.writer = new PolygonWriter(new NullableMapWriter(vector), X_EXTERIOR_FIELD, Y_EXTERIOR_FIELD, X_INTERIOR_FIELD, Y_INTERIOR_FIELD);
    this.reader = new PolygonReader(vector, X_EXTERIOR_FIELD, Y_EXTERIOR_FIELD, X_INTERIOR_FIELD, Y_INTERIOR_FIELD);
  }

  @Override
  public PolygonWriter getWriter() {
    return writer;
  }

  @Override
  public PolygonReader getReader() {
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
