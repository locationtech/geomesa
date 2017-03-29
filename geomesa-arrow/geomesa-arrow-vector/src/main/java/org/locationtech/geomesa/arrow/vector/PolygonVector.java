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
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
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

public class PolygonVector implements GeometryVector<Polygon> {

  private static final String X_EXTERIOR_FIELD = "x-ext";
  private static final String Y_EXTERIOR_FIELD = "y-ext";
  private static final String X_INTERIOR_FIELD = "x-int";
  private static final String Y_INTERIOR_FIELD = "y-int";

  static final List<Field> fields =
    Collections.unmodifiableList(new ArrayList<>(Arrays.asList(
      new Field(X_EXTERIOR_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_FIELD),
      new Field(Y_EXTERIOR_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_FIELD),
      new Field(X_INTERIOR_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_DOUBLE_FIELD),
      new Field(Y_INTERIOR_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_DOUBLE_FIELD)
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
    this.writer = new PolygonWriter(new NullableMapWriter(vector));
    this.reader = new PolygonReader(vector);
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

  public static class PolygonWriter extends BaseGeometryWriter<Polygon> implements GeometryWriter<Polygon> {

    private final ListWriter xExteriorWriter;
    private final ListWriter yExteriorWriter;
    private final ListWriter xInteriorWriter;
    private final ListWriter yInteriorWriter;

    public PolygonWriter(MapWriter writer) {
      super(writer);
      this.xExteriorWriter = writer.list(X_EXTERIOR_FIELD);
      this.yExteriorWriter = writer.list(Y_EXTERIOR_FIELD);
      this.xInteriorWriter = writer.list(X_INTERIOR_FIELD);
      this.yInteriorWriter = writer.list(Y_INTERIOR_FIELD);
    }

    @Override
    protected void writeGeometry(Polygon geom) {
      xExteriorWriter.startList();
      yExteriorWriter.startList();
      xInteriorWriter.startList();
      yInteriorWriter.startList();

      LineString exterior = geom.getExteriorRing();
      for (int i = 0; i < exterior.getNumPoints(); i++) {
        Coordinate p = exterior.getCoordinateN(i);
        xExteriorWriter.float8().writeFloat8(p.x);
        yExteriorWriter.float8().writeFloat8(p.y);
      }

      ListWriter xInner = xInteriorWriter.list();
      ListWriter yInner = yInteriorWriter.list();
      for (int i = 0; i < geom.getNumInteriorRing(); i++) {
        LineString interior = geom.getInteriorRingN(i);
        xInner.startList();
        yInner.startList();
        for (int j = 0; j < interior.getNumPoints(); j++) {
          Coordinate p = interior.getCoordinateN(j);
          xInner.float8().writeFloat8(p.x);
          yInner.float8().writeFloat8(p.y);
        }
        xInner.endList();
        yInner.endList();
      }

      xExteriorWriter.endList();
      yExteriorWriter.endList();
      xInteriorWriter.endList();
      yInteriorWriter.endList();
    }
  }

  public static class PolygonReader extends BaseGeometryReader<Polygon> implements GeometryReader<Polygon> {

    private final ListVector.Accessor xExteriorAccessor;
    private final ListVector.Accessor yExteriorAccessor;
    private final ListVector.Accessor xInteriorAccessor;
    private final ListVector.Accessor yInteriorAccessor;

    public PolygonReader(NullableMapVector vector) {
      super(vector);
      this.xExteriorAccessor = (ListVector.Accessor) vector.getChild(X_EXTERIOR_FIELD).getAccessor();
      this.yExteriorAccessor = (ListVector.Accessor) vector.getChild(Y_EXTERIOR_FIELD).getAccessor();
      this.xInteriorAccessor = (ListVector.Accessor) vector.getChild(X_INTERIOR_FIELD).getAccessor();
      this.yInteriorAccessor = (ListVector.Accessor) vector.getChild(Y_INTERIOR_FIELD).getAccessor();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Polygon readGeometry(int index) {
      List<Double> xs = (List<Double>) xExteriorAccessor.getObject(index);
      List<Double> ys = (List<Double>) yExteriorAccessor.getObject(index);
      if (xs.size() != ys.size()) {
        throw new IllegalArgumentException("Invalid point vectors: x: " + xs.size() + " y: " + ys.size());
      }
      Coordinate[] coordinates = new Coordinate[xs.size()];
      for (int i = 0; i < coordinates.length; i++) {
        coordinates[i] = new Coordinate(xs.get(i), ys.get(i));
      }
      List<List<Double>> xInts = (List<List<Double>>) xInteriorAccessor.getObject(index);
      List<List<Double>> yInts = (List<List<Double>>) yInteriorAccessor.getObject(index);
      int xCount = xInts == null ? 0 : xInts.size();
      int yCount = yInts == null ? 0 : yInts.size();
      if (xCount != yCount) {
        throw new IllegalArgumentException("Invalid interior vectors: x: " + xCount + " y: " + yCount);
      }
      if (xCount == 0) {
        // no interior holes
        return factory.createPolygon(coordinates);
      } else {
        LinearRing exterior = factory.createLinearRing(coordinates);
        LinearRing[] holes = new LinearRing[xInts.size()];
        for (int i = 0; i < holes.length; i++) {
          List<Double> x = xInts.get(i);
          List<Double> y = yInts.get(i);
          if (x.size() != y.size()) {
            throw new IllegalArgumentException("Invalid interior vectors: x: " + x.size() + " y: " + y.size());
          }
          Coordinate[] c = new Coordinate[x.size()];
          for (int j = 0; j < c.length; j++) {
            c[j] = new Coordinate(x.get(j), y.get(j));
          }
          holes[i] = factory.createLinearRing(c);
        }
        return factory.createPolygon(exterior, holes);
      }
    }
  }

}
