/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Point;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.util.ArrowHelper;
import org.locationtech.geomesa.arrow.vector.util.BaseGeometryReader;
import org.locationtech.geomesa.arrow.vector.util.BaseGeometryWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PointVector implements GeometryVector<Point> {

  private static final String X_FIELD = "x";
  private static final String Y_FIELD = "y";

  // fields created by this vector - should be unique, used for identifying geometry type from a schema
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
    // these should match the the 'fields' variable
    vector.addOrGet(X_FIELD, MinorType.FLOAT8, NullableFloat8Vector.class, null);
    vector.addOrGet(Y_FIELD, MinorType.FLOAT8, NullableFloat8Vector.class, null);
    this.writer = new PointWriter(new NullableMapWriter(vector));
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

  public static class PointWriter extends BaseGeometryWriter<Point> {

    private final Float8Writer xWriter;
    private final Float8Writer yWriter;

    public PointWriter(MapWriter writer) {
      super(writer);
      this.xWriter = writer.float8(X_FIELD);
      this.yWriter = writer.float8(Y_FIELD);
    }

    @Override
    protected void writeGeometry(Point geom) {
      xWriter.writeFloat8(geom.getX());
      yWriter.writeFloat8(geom.getY());
    }
  }

  public static class PointReader extends BaseGeometryReader<Point> {

    private final NullableFloat8Vector.Accessor xAccessor;
    private final NullableFloat8Vector.Accessor yAccessor;

    public PointReader(NullableMapVector vector) {
      super(vector);
      this.xAccessor = (NullableFloat8Vector.Accessor) vector.getChild(X_FIELD).getAccessor();
      this.yAccessor = (NullableFloat8Vector.Accessor) vector.getChild(Y_FIELD).getAccessor();
    }

    @Override
    protected Point readGeometry(int index) {
      double x = xAccessor.getObject(index);
      double y = yAccessor.getObject(index);
      return factory.createPoint(new Coordinate(x, y));
    }
  }
}
