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
import com.vividsolutions.jts.geom.MultiLineString;
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

public class MultiLineStringVector implements GeometryVector<MultiLineString> {

  private static final String X_FIELD = "xx";
  private static final String Y_FIELD = "yy";

  // fields created by this vector - should be unique, used for identifying geometry type from a schema
  static final List<Field> fields =
    Collections.unmodifiableList(new ArrayList<>(Arrays.asList(
      new Field(X_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_DOUBLE_FIELD),
      new Field(Y_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_DOUBLE_FIELD)
    )));

  private final NullableMapVector vector;
  private final MultiLineStringWriter writer;
  private final MultiLineStringReader reader;

  public MultiLineStringVector(String name, BufferAllocator allocator) {
    this(new NullableMapVector(name, allocator, null, null));
    this.vector.allocateNew();
  }

  public MultiLineStringVector(NullableMapVector vector) {
    this.vector = vector;
    // create the fields we will write to up front
    // these should match the the 'fields' variable
    ((ListVector) vector.addOrGet(X_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.LIST, null).getVector()).addOrGetVector(MinorType.FLOAT8, null);
    ((ListVector) vector.addOrGet(Y_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.LIST, null).getVector()).addOrGetVector(MinorType.FLOAT8, null);
    this.writer = new MultiLineStringWriter(new NullableMapWriter(vector));
    this.reader = new MultiLineStringReader(vector);
  }

  @Override
  public MultiLineStringWriter getWriter() {
    return writer;
  }

  @Override
  public MultiLineStringReader getReader() {
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

  public static class MultiLineStringWriter extends BaseGeometryWriter<MultiLineString> {

    private final ListWriter xWriter;
    private final ListWriter yWriter;

    public MultiLineStringWriter(MapWriter writer) {
      super(writer);
      this.xWriter = writer.list(X_FIELD);
      this.yWriter = writer.list(Y_FIELD);
    }

    @Override
    protected void writeGeometry(MultiLineString geom) {
      xWriter.startList();
      yWriter.startList();
      ListWriter xInner = xWriter.list();
      ListWriter yInner = yWriter.list();
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        LineString linestring = (LineString) geom.getGeometryN(i);
        xInner.startList();
        yInner.startList();
        for (int j = 0; j < linestring.getNumPoints(); j++) {
          Point p = linestring.getPointN(j);
          xInner.float8().writeFloat8(p.getX());
          yInner.float8().writeFloat8(p.getY());
        }
        xInner.endList();
        yInner.endList();
      }
      xWriter.endList();
      yWriter.endList();
    }
  }

  public static class MultiLineStringReader extends BaseGeometryReader<MultiLineString> {

    private final ListVector.Accessor xAccessor;
    private final ListVector.Accessor yAccessor;

    public MultiLineStringReader(NullableMapVector vector) {
      super(vector);
      this.xAccessor = (ListVector.Accessor) vector.getChild(X_FIELD).getAccessor();
      this.yAccessor = (ListVector.Accessor) vector.getChild(Y_FIELD).getAccessor();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected MultiLineString readGeometry(int index) {
      List<List<Double>> xx = (List<List<Double>>) xAccessor.getObject(index);
      List<List<Double>> yy = (List<List<Double>>) yAccessor.getObject(index);
      if (xx.size() != yy.size()) {
        throw new IllegalArgumentException("Invalid multi line string vectors: xx: " + xx.size() + " yy: " + yy.size());
      }
      LineString[] linestrings = new LineString[xx.size()];
      for (int i = 0; i < linestrings.length; i++) {
        List<Double> x = xx.get(i);
        List<Double> y = yy.get(i);
        if (x.size() != y.size()) {
          throw new IllegalArgumentException("Invalid multi line string vectors: x: " + x.size() + " y: " + y.size());
        }
        Coordinate[] coordinates = new Coordinate[x.size()];
        for (int j = 0; j < coordinates.length; j++) {
          coordinates[j] = new Coordinate(x.get(j), y.get(j));
        }
        linestrings[i] = factory.createLineString(coordinates);
      }
      return factory.createMultiLineString(linestrings);
    }
  }
}
