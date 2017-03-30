/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.util.ArrowHelper;
import org.locationtech.geomesa.arrow.vector.util.BaseGeometryReader;
import org.locationtech.geomesa.arrow.vector.util.BaseGeometryWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LineStringVector implements GeometryVector<LineString, NullableMapVector> {

  private static final String X_FIELD = "x";
  private static final String Y_FIELD = "y";

  // fields created by this vector - should be unique, used for identifying geometry type from a schema
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
    vector.initializeChildrenFromFields(fields);
    this.writer = new LineStringWriter(new NullableMapWriter(vector));
    this.reader = new LineStringReader(vector);
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

  public static class LineStringWriter extends BaseGeometryWriter<LineString> {

    private final ListWriter xWriter;
    private final ListWriter yWriter;

    public LineStringWriter(MapWriter writer) {
      super(writer);
      this.xWriter = writer.list(X_FIELD);
      this.yWriter = writer.list(Y_FIELD);
    }

    @Override
    protected void writeGeometry(LineString geom) {
      xWriter.startList();
      yWriter.startList();
      for (int i = 0; i < geom.getNumPoints(); i++) {
        Coordinate p = geom.getCoordinateN(i);
        xWriter.float8().writeFloat8(p.x);
        yWriter.float8().writeFloat8(p.y);
      }
      xWriter.endList();
      yWriter.endList();
    }
  }

  public static class LineStringReader extends BaseGeometryReader<LineString> {

    private final ListVector.Accessor xAccessor;
    private final ListVector.Accessor yAccessor;

    public LineStringReader(NullableMapVector vector) {
      super(vector);
      this.xAccessor = (ListVector.Accessor) vector.getChild(X_FIELD).getAccessor();
      this.yAccessor = (ListVector.Accessor) vector.getChild(Y_FIELD).getAccessor();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected LineString readGeometry(int index) {
      List<Double> x = (List<Double>) xAccessor.getObject(index);
      List<Double> y = (List<Double>) yAccessor.getObject(index);
      if (x.size() != y.size()) {
        throw new IllegalArgumentException("Invalid line string vectors: x: " + x.size() + " y: " + y.size());
      }
      Coordinate[] coordinates = new Coordinate[x.size()];
      for (int i = 0; i < coordinates.length; i++) {
        coordinates[i] = new Coordinate(x.get(i), y.get(i));
      }
      return factory.createLineString(coordinates);
    }
  }
}
