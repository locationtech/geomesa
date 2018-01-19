/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector.impl;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.locationtech.geomesa.arrow.vector.GeometryVector;

import java.util.List;
import java.util.Map;

public abstract class AbstractPointVector implements GeometryVector<Point, FixedSizeListVector> {

  public static FieldType createFieldType(Map<String, String> metadata) {
    return new FieldType(true, new ArrowType.FixedSizeList(2), null, metadata);
  }

  private final FixedSizeListVector vector;
  private final PointWriter writer;
  private final PointReader reader;

  protected AbstractPointVector(String name, BufferAllocator allocator, Map<String, String> metadata) {
    this(new FixedSizeListVector(name, allocator, createFieldType(metadata), null));
  }

  protected AbstractPointVector(String name, AbstractContainerVector container, Map<String, String> metadata) {
    this(container.addOrGet(name, createFieldType(metadata), FixedSizeListVector.class));
  }

  protected AbstractPointVector(FixedSizeListVector vector) {
    // create the fields we will write to up front
    if (vector.getDataVector().equals(ZeroVector.INSTANCE)) {
      vector.initializeChildrenFromFields(getFields());
      vector.allocateNew();
    }
    this.vector = vector;
    this.writer = createWriter(vector);
    this.reader = createReader(vector);
  }

  protected abstract List<Field> getFields();
  protected abstract PointWriter createWriter(FixedSizeListVector vector);
  protected abstract PointReader createReader(FixedSizeListVector vector);

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
    vector.close();
  }

  @Override
  public void transfer(int fromIndex, int toIndex, GeometryVector<Point, FixedSizeListVector> to) {
    PointWriter writer = (PointWriter) to.getWriter();
    if (reader.accessor.isNull(fromIndex)) {
      writer.mutator.setNull(toIndex);
    } else {
      writer.mutator.setNotNull(toIndex);
      writer.writeOrdinal(toIndex * 2, reader.readOrdinal(fromIndex * 2));
      writer.writeOrdinal(toIndex * 2 + 1, reader.readOrdinal(fromIndex * 2 + 1));
    }
  }

  public static abstract class PointWriter extends AbstractGeometryWriter<Point> {

    private final FixedSizeListVector.Mutator mutator;

    protected PointWriter(FixedSizeListVector vector) {
      this.mutator = vector.getMutator();
      setOrdinalMutator(vector.getChildrenFromFields().get(0).getMutator());
    }

    @Override
    public void set(int index, Point geom) {
      if (geom == null) {
        mutator.setNull(index);
      } else {
        mutator.setNotNull(index);
        writeOrdinal(index * 2, geom.getY());
        writeOrdinal(index * 2 + 1, geom.getX());
      }
    }

    @Override
    public void setValueCount(int count) {
      mutator.setValueCount(count);
    }
  }

  public static abstract class PointReader extends AbstractGeometryReader<Point> {

    private static final GeometryFactory factory = new GeometryFactory();

    private final FixedSizeListVector.Accessor accessor;

    protected PointReader(FixedSizeListVector vector) {
      this.accessor = vector.getAccessor();
      setOrdinalAccessor(vector.getChildrenFromFields().get(0).getAccessor());
    }

    @Override
    public Point get(int index) {
      if (accessor.isNull(index)) {
        return null;
      } else {
        double y = readOrdinal(index * 2);
        double x = readOrdinal(index * 2 + 1);
        return factory.createPoint(new Coordinate(x, y));
      }
    }

    /**
     * Specialized read methods to return a single ordinate at a time. Does not check for null values.
     *
     * @param index index of the ordinate to read
     * @return y ordinate
     */
    public double getCoordinateY(int index) {
      return readOrdinal(index * 2);

    }

    /**
     * Specialized read methods to return a single ordinate at a time. Does not check for null values.
     *
     * @param index index of the ordinate to read
     * @return x ordinate
     */
    public double getCoordinateX(int index) {
      return readOrdinal(index * 2 + 1);
    }

    @Override
    public int getValueCount() {
      return accessor.getValueCount();
    }

    @Override
    public int getNullCount() {
      int count = accessor.getNullCount();
      if (count < 0) {
        return 0;
      } else {
        return count;
      }
    }
  }
}
