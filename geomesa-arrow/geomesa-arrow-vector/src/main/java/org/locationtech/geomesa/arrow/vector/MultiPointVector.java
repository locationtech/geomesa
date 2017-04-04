/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.util.ArrowHelper;
import org.locationtech.geomesa.arrow.vector.util.BaseGeometryReader;
import org.locationtech.geomesa.arrow.vector.util.BaseGeometryWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MultiPointVector implements GeometryVector<MultiPoint> {

  private static final String X_FIELD = "xx";
  private static final String Y_FIELD = "yy";

  // fields created by this vector - should be unique, used for identifying geometry type from a schema
  static final List<Field> fields =
    Collections.unmodifiableList(new ArrayList<>(Arrays.asList(
      new Field(X_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_FIELD),
      new Field(Y_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_FIELD)
    )));

  private final NullableMapVector vector;
  private final MultiPointWriter writer;
  private final MultiPointReader reader;

  public MultiPointVector(String name, BufferAllocator allocator) {
    this(new NullableMapVector(name, allocator, null, null));
    this.vector.allocateNew();
  }

  public MultiPointVector(NullableMapVector vector) {
    this.vector = vector;
    // create the fields we will write to up front
    // these should match the the 'fields' variable
    vector.addOrGet(X_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.FLOAT8, null);
    vector.addOrGet(Y_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.FLOAT8, null);
    this.writer = new MultiPointWriter(new NullableMapWriter(vector));
    this.reader = new MultiPointReader(vector);
  }

  @Override
  public MultiPointWriter getWriter() {
    return writer;
  }

  @Override
  public MultiPointReader getReader() {
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

  public static class MultiPointWriter extends BaseGeometryWriter<MultiPoint> {

    private final ListWriter xWriter;
    private final ListWriter yWriter;

    public MultiPointWriter(MapWriter writer) {
      super(writer);
      this.xWriter = writer.list(X_FIELD);
      this.yWriter = writer.list(Y_FIELD);
    }

    @Override
    protected void writeGeometry(MultiPoint geom) {
      xWriter.startList();
      yWriter.startList();
      for (int i = 0; i < geom.getNumPoints(); i++) {
        Point p = (Point) geom.getGeometryN(i);
        xWriter.float8().writeFloat8(p.getX());
        yWriter.float8().writeFloat8(p.getY());
      }
      xWriter.endList();
      yWriter.endList();
    }
  }

  public static class MultiPointReader extends BaseGeometryReader<MultiPoint> {

    private final ListVector.Accessor xAccessor;
    private final ListVector.Accessor yAccessor;

    public MultiPointReader(NullableMapVector vector) {
      super(vector);
      this.xAccessor = (ListVector.Accessor) vector.getChild(X_FIELD).getAccessor();
      this.yAccessor = (ListVector.Accessor) vector.getChild(Y_FIELD).getAccessor();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected MultiPoint readGeometry(int index) {
      List<Double> x = (List<Double>) xAccessor.getObject(index);
      List<Double> y = (List<Double>) yAccessor.getObject(index);
      if (x.size() != y.size()) {
        throw new IllegalArgumentException("Invalid multi point vectors: x: " + x.size() + " y: " + y.size());
      }
      Coordinate[] coordinates = new Coordinate[x.size()];
      for (int i = 0; i < coordinates.length; i++) {
        coordinates[i] = new Coordinate(x.get(i), y.get(i));
      }
      return factory.createMultiPoint(coordinates);
    }
  }
}
