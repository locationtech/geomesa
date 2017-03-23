/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector.writer;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;

public class PolygonWriter implements GeometryWriter<Polygon> {

  private final MapWriter writer;
  private final ListWriter xExteriorWriter;
  private final ListWriter yExteriorWriter;
  private final ListWriter xInteriorWriter;
  private final ListWriter yInteriorWriter;

  public PolygonWriter(MapWriter writer, String xExtField, String yExtField, String xIntField, String yIntField) {
    this.writer = writer;
    this.xExteriorWriter = writer.list(xExtField);
    this.yExteriorWriter = writer.list(yExtField);
    this.xInteriorWriter = writer.list(xIntField);
    this.yInteriorWriter = writer.list(yIntField);
  }

  @Override
  public void set(Polygon geom) {
    if (geom != null) {
      writer.start();
      xExteriorWriter.startList();
      yExteriorWriter.startList();
      LineString exterior = geom.getExteriorRing();
      for (int i = 0; i < exterior.getNumPoints(); i++) {
        Coordinate p = exterior.getCoordinateN(i);
        xExteriorWriter.float8().writeFloat8(p.x);
        yExteriorWriter.float8().writeFloat8(p.y);
      }
      xExteriorWriter.endList();
      yExteriorWriter.endList();
      xInteriorWriter.startList();
      yInteriorWriter.startList();
      for (int i = 0; i < geom.getNumInteriorRing(); i++) {
        LineString interior = geom.getInteriorRingN(i);
        ListWriter xInner = xInteriorWriter.list();
        ListWriter yInner = yInteriorWriter.list();
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
      xInteriorWriter.endList();
      yInteriorWriter.endList();
      writer.end();
    }
  }

  @Override
  public void set(int i, Polygon geom) {
    writer.setPosition(i);
    set(geom);
  }

  @Override
  public void setValueCount(int count) {
    if (writer instanceof NullableMapWriter) {
      ((NullableMapWriter) writer).setValueCount(count);
    } else {
      throw new RuntimeException("Not nullable writer: " + writer);
    }
  }

  @Override
  public void close() throws Exception {
    writer.close();
  }
}
