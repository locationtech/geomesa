/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector.reader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NullableMapVector;

import java.util.List;

public class MultiLineStringReader implements GeometryReader<MultiLineString> {

  private final NullableMapVector.Accessor accessor;
  private final ListVector.Accessor xAccessor;
  private final ListVector.Accessor yAccessor;

  public MultiLineStringReader(NullableMapVector vector, String xField, String yField) {
    this.accessor = vector.getAccessor();
    this.xAccessor = (ListVector.Accessor) vector.getChild(xField).getAccessor();
    this.yAccessor = (ListVector.Accessor) vector.getChild(yField).getAccessor();
  }

  @Override
  @SuppressWarnings("unchecked")
  public MultiLineString get(int i) {
    if (accessor.isNull(i)) {
      return null;
    } else {
      List<List<Double>> xx = (List<List<Double>>) xAccessor.getObject(i);
      List<List<Double>> yy = (List<List<Double>>) yAccessor.getObject(i);
      LineString[] linestrings = new LineString[xx.size()];
      for (int j = 0; j < linestrings.length; j++) {
        List<Double> x = xx.get(j);
        List<Double> y = yy.get(j);
        Coordinate[] coordinates = new Coordinate[x.size()];
        for (int k = 0; k < coordinates.length; k++) {
          coordinates[k] = new Coordinate(x.get(k), y.get(k));
        }
        linestrings[j] = factory.createLineString(coordinates);
      }
      return factory.createMultiLineString(linestrings);
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
