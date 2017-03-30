/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.util.ArrowHelper;

import java.util.List;

public class PointVector implements GeometryVector<Point, FixedSizeListVector> {

  // fields created by this vector
  static final List<Field> fields = ArrowHelper.XY_FIELD;

  private final FixedSizeListVector vector;
  private final PointWriter writer;
  private final PointReader reader;

  public PointVector(String name, BufferAllocator allocator) {
    this(new FixedSizeListVector(name, allocator, 2, null));
    this.vector.allocateNew();
  }

  public PointVector(FixedSizeListVector vector) {
    this.vector = vector;
    // create the fields we will write to up front
    vector.initializeChildrenFromFields(fields);
    this.writer = new PointWriter(vector);
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
  public FixedSizeListVector getVector() {
    return vector;
  }

  @Override
  public void close() throws Exception {
    writer.close();
    reader.close();
    vector.close();
  }

  public static class PointWriter implements GeometryWriter<Point> {

    private final UnionListWriter writer;

    public PointWriter(FixedSizeListVector vector) {
      this.writer = vector.getWriter();
    }

    @Override
    public void set(Point geom) {
      if (geom != null) {
        writer.startList();
        writer.writeFloat4((float) geom.getY());
        writer.writeFloat4((float) geom.getX());
        writer.endList();
      }
    }

    @Override
    public void set(int i, Point geom) {
      writer.setPosition(i);
      set(geom);
    }

    @Override
    public void setValueCount(int count) {
      writer.setValueCount(count);
    }

    @Override
    public void close() throws Exception {
      writer.close();
    }
  }

  public static class PointReader implements GeometryReader<Point> {

    private static final GeometryFactory factory = new GeometryFactory();

    private final UnionFixedSizeListReader reader;
    private final FieldReader subReader;
    private final FixedSizeListVector.Accessor accessor;

    public PointReader(FixedSizeListVector vector) {
      this.reader = vector.getReader();
      this.subReader = reader.reader();
      this.accessor = vector.getAccessor();
    }

    @Override
    public Point get(int index) {
      reader.setPosition(index);
      if (reader.isSet()) {
        reader.next();
        float y = subReader.readFloat();
        reader.next();
        float x = subReader.readFloat();
        return factory.createPoint(new Coordinate(x, y));
      } else {
        return null;
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
    public void close() throws Exception {}
  }
}
