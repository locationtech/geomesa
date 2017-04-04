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
import com.vividsolutions.jts.geom.MultiPolygon;
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

public class MultiPolygonVector implements GeometryVector<MultiPolygon> {

  private static final String X_EXTERIOR_FIELD = "x-ext";
  private static final String Y_EXTERIOR_FIELD = "y-ext";
  private static final String X_INTERIOR_FIELD = "x-int";
  private static final String Y_INTERIOR_FIELD = "y-int";

  // fields created by this vector - should be unique, used for identifying geometry type from a schema
  static final List<Field> fields =
    Collections.unmodifiableList(new ArrayList<>(Arrays.asList(
      new Field(X_EXTERIOR_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_DOUBLE_FIELD),
      new Field(Y_EXTERIOR_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.DOUBLE_DOUBLE_FIELD),
      new Field(X_INTERIOR_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.TRIPLE_DOUBLE_FIELD),
      new Field(Y_INTERIOR_FIELD, true, ArrowType.List.INSTANCE, ArrowHelper.TRIPLE_DOUBLE_FIELD)
    )));

  private final NullableMapVector vector;
  private final MultiPolygonWriter writer;
  private final MultiPolygonReader reader;

  public MultiPolygonVector(String name, BufferAllocator allocator) {
    this(new NullableMapVector(name, allocator, null, null));
    this.vector.allocateNew();
  }

  public MultiPolygonVector(NullableMapVector vector) {
    this.vector = vector;
    // create the fields we will write to up front
    // these should match the the 'fields' variable
    ((ListVector) vector.addOrGet(X_EXTERIOR_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.LIST, null).getVector()).addOrGetVector(MinorType.FLOAT8, null);
    ((ListVector) vector.addOrGet(Y_EXTERIOR_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.LIST, null).getVector()).addOrGetVector(MinorType.FLOAT8, null);
    ((ListVector) ((ListVector) vector.addOrGet(X_INTERIOR_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.LIST, null).getVector()).addOrGetVector(MinorType.LIST, null).getVector()).addOrGetVector(MinorType.FLOAT8, null);
    ((ListVector) ((ListVector) vector.addOrGet(Y_INTERIOR_FIELD, MinorType.LIST, ListVector.class, null).addOrGetVector(MinorType.LIST, null).getVector()).addOrGetVector(MinorType.LIST, null).getVector()).addOrGetVector(MinorType.FLOAT8, null);
    this.writer = new MultiPolygonWriter(new NullableMapWriter(vector));
    this.reader = new MultiPolygonReader(vector);
  }

  @Override
  public MultiPolygonWriter getWriter() {
    return writer;
  }

  @Override
  public MultiPolygonReader getReader() {
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

  public static class MultiPolygonWriter extends BaseGeometryWriter<MultiPolygon> {

    private final ListWriter xExteriorWriter;
    private final ListWriter yExteriorWriter;
    private final ListWriter xInteriorWriter;
    private final ListWriter yInteriorWriter;

    public MultiPolygonWriter(MapWriter writer) {
      super(writer);
      this.xExteriorWriter = writer.list(X_EXTERIOR_FIELD);
      this.yExteriorWriter = writer.list(Y_EXTERIOR_FIELD);
      this.xInteriorWriter = writer.list(X_INTERIOR_FIELD);
      this.yInteriorWriter = writer.list(Y_INTERIOR_FIELD);
    }

    @Override
    protected void writeGeometry(MultiPolygon geom) {
      xExteriorWriter.startList();
      yExteriorWriter.startList();
      xInteriorWriter.startList();
      yInteriorWriter.startList();

      ListWriter xExtInner = xExteriorWriter.list();
      ListWriter yExtInner = yExteriorWriter.list();
      ListWriter xIntInner = xInteriorWriter.list();
      ListWriter yIntInner = yInteriorWriter.list();

      ListWriter xIntInnerInner = xIntInner.list();
      ListWriter yIntInnerInner = yIntInner.list();

      for (int i = 0; i < geom.getNumGeometries(); i++) {
        Polygon poly = (Polygon) geom.getGeometryN(i);

        LineString exterior = poly.getExteriorRing();
        xExtInner.startList();
        yExtInner.startList();
        for (int j = 0; j < exterior.getNumPoints(); j++) {
          Coordinate p = exterior.getCoordinateN(j);
          xExtInner.float8().writeFloat8(p.x);
          yExtInner.float8().writeFloat8(p.y);
        }
        xExtInner.endList();
        yExtInner.endList();

        xIntInner.startList();
        yIntInner.startList();
        for (int j = 0; j < poly.getNumInteriorRing(); j++) {
          LineString interior = poly.getInteriorRingN(j);
          xIntInnerInner.startList();
          yIntInnerInner.startList();
          for (int k = 0; k < interior.getNumPoints(); k++) {
            Coordinate p = interior.getCoordinateN(k);
            xIntInnerInner.float8().writeFloat8(p.x);
            yIntInnerInner.float8().writeFloat8(p.y);
          }
          xIntInnerInner.endList();
          yIntInnerInner.endList();
        }
        xIntInner.endList();
        yIntInner.endList();
      }
      xExteriorWriter.endList();
      yExteriorWriter.endList();
      xInteriorWriter.endList();
      yInteriorWriter.endList();
    }
  }

  public static class MultiPolygonReader extends BaseGeometryReader<MultiPolygon> {

    private final ListVector.Accessor xExteriorAccessor;
    private final ListVector.Accessor yExteriorAccessor;
    private final ListVector.Accessor xInteriorAccessor;
    private final ListVector.Accessor yInteriorAccessor;

    public MultiPolygonReader(NullableMapVector vector) {
      super(vector);
      this.xExteriorAccessor = (ListVector.Accessor) vector.getChild(X_EXTERIOR_FIELD).getAccessor();
      this.yExteriorAccessor = (ListVector.Accessor) vector.getChild(Y_EXTERIOR_FIELD).getAccessor();
      this.xInteriorAccessor = (ListVector.Accessor) vector.getChild(X_INTERIOR_FIELD).getAccessor();
      this.yInteriorAccessor = (ListVector.Accessor) vector.getChild(Y_INTERIOR_FIELD).getAccessor();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected MultiPolygon readGeometry(int index) {
      List<List<Double>> xx = (List<List<Double>>) xExteriorAccessor.getObject(index);
      List<List<Double>> yy = (List<List<Double>>) yExteriorAccessor.getObject(index);
      List<List<List<Double>>> xxxInts = (List<List<List<Double>>>) xInteriorAccessor.getObject(index);
      List<List<List<Double>>> yyyInts = (List<List<List<Double>>>) yInteriorAccessor.getObject(index);

      if (xx.size() != yy.size()) {
        throw new IllegalArgumentException("Invalid multi polygon vectors: xx: " + xx.size() + " yy: " + yy.size());
      } else if (xxxInts.size() != yyyInts.size()) {
        throw new IllegalArgumentException("Invalid multi polygon vectors: xxx-int: " + xxxInts.size() + " yyy-int: " + yyyInts.size());
      }

      Polygon[] polygons = new Polygon[xx.size()];

      for (int i = 0; i < polygons.length; i++) {
        List<Double> x = xx.get(i);
        List<Double> y = yy.get(i);
        List<List<Double>> xxInt = xxxInts.get(i);
        List<List<Double>> yyInt = yyyInts.get(i);

        if (x.size() != y.size()) {
          throw new IllegalArgumentException("Invalid multi polygon vectors: x: " + x.size() + " y: " + y.size());
        } else if (xxInt.size() != yyInt.size()) {
          throw new IllegalArgumentException("Invalid multi polygon interior vectors: xx-int: " + xxInt.size() + " yy-int: " + yyInt.size());
        }

        Coordinate[] exteriorCoords = new Coordinate[x.size()];
        for (int j = 0; j < exteriorCoords.length; j++) {
          exteriorCoords[j] = new Coordinate(x.get(j), y.get(j));
        }

        if (xxInt.size() == 0) {
          // no interior holes
          polygons[i] = factory.createPolygon(exteriorCoords);
        } else {
          LinearRing exterior = factory.createLinearRing(exteriorCoords);
          LinearRing[] holes = new LinearRing[xxInt.size()];
          for (int j = 0; j < holes.length; j++) {
            List<Double> xInt = xxInt.get(j);
            List<Double> yInt = yyInt.get(j);
            if (xInt.size() != yInt.size()) {
              throw new IllegalArgumentException("Invalid multi polygon interior vectors: x: " + xInt.size() + " y: " + yInt.size());
            }
            Coordinate[] interiorCoords = new Coordinate[xInt.size()];
            for (int k = 0; k < interiorCoords.length; k++) {
              interiorCoords[k] = new Coordinate(xInt.get(k), yInt.get(k));
            }
            holes[j] = factory.createLinearRing(interiorCoords);
          }
          polygons[i] = factory.createPolygon(exterior, holes);
        }
      }
      return factory.createMultiPolygon(polygons);
    }
  }
}
