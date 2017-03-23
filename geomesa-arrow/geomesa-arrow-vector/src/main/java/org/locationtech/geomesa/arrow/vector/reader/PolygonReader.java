/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector.reader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NullableMapVector;

import java.util.List;

public class PolygonReader implements GeometryReader<Polygon> {

  private final NullableMapVector.Accessor accessor;
  private final ListVector.Accessor xExteriorAccessor;
  private final ListVector.Accessor yExteriorAccessor;
  private final ListVector.Accessor xInteriorAccessor;
  private final ListVector.Accessor yInteriorAccessor;

  public PolygonReader(NullableMapVector vector, String xExtField, String yExtField, String xIntField, String yIntField) {
    this.accessor = vector.getAccessor();
    this.xExteriorAccessor = (ListVector.Accessor) vector.getChild(xExtField).getAccessor();
    this.yExteriorAccessor = (ListVector.Accessor) vector.getChild(yExtField).getAccessor();
    this.xInteriorAccessor = (ListVector.Accessor) vector.getChild(xIntField).getAccessor();
    this.yInteriorAccessor = (ListVector.Accessor) vector.getChild(yIntField).getAccessor();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Polygon get(int index) {
    if (accessor.isNull(index)) {
      return null;
    } else {
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
      if (xInts == null || xInts.isEmpty()) {
        return factory.createPolygon(coordinates);
      } else {
        if (xInts.size() != yInts.size()) {
          throw new IllegalArgumentException("Invalid interior vectors: x: " + xInts.size() + " y: " + yInts.size());
        }
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

  @Override
  public int getValueCount() {
    return accessor.getValueCount();
  }

  @Override
  public int getNullCount() {
    return accessor.getNullCount();
  }

  @Override
  public void close() throws Exception {
  }
}
