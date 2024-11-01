/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.jts.impl;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.locationtech.geomesa.arrow.jts.GeometryVector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;

import java.util.Map;

public abstract class AbstractPointVector<T extends FieldVector>
    extends AbstractGeometryVector<Point, FixedSizeListVector, T> {

  public static FieldType createFieldType(Map<String, String> metadata) {
    return new FieldType(true, new ArrowType.FixedSizeList(2), null, metadata);
  }

  protected AbstractPointVector(String name, BufferAllocator allocator, Map<String, String> metadata) {
    this(new FixedSizeListVector(name, allocator, createFieldType(metadata), null));
  }

  protected AbstractPointVector(String name, AbstractContainerVector container, Map<String, String> metadata) {
    this(container.addOrGet(name, createFieldType(metadata), FixedSizeListVector.class));
  }

  @SuppressWarnings("unchecked")
  protected AbstractPointVector(FixedSizeListVector vector) {
    super(vector);
    // create the fields we will write to up front
    if (vector.getDataVector() == ZeroVector.INSTANCE) {
      vector.initializeChildrenFromFields(getFields());
      vector.allocateNew();
    }
    setOrdinalVector((T) vector.getChildrenFromFields().get(0));
  }

  @Override
  public void set(int index, Point geom) {
    if (geom == null) {
      vector.setNull(index);
    } else {
      vector.setNotNull(index);
      if (getFlipAxisOrder()) {
        writeOrdinal(y(index), geom.getX());
        writeOrdinal(x(index), geom.getY());
      } else {
        writeOrdinal(y(index), geom.getY());
        writeOrdinal(x(index), geom.getX());
      }
    }
  }

  @Override
  public Point get(int index) {
    if (vector.isNull(index)) {
      return null;
    } else {
      final double y, x;
      if (getFlipAxisOrder()) {
        y = x(index);
        x = y(index);
      } else {
        y = y(index);
        x = x(index);
      }
      return factory.createPoint(new Coordinate(x, y));
    }
  }

  @Override
  public void transfer(int fromIndex, int toIndex, GeometryVector<Point, FixedSizeListVector> to) {
    AbstractPointVector typed = (AbstractPointVector) to;
    if (vector.isNull(fromIndex)) {
      ((FixedSizeListVector) typed.vector).setNull(toIndex);
    } else {
      ((FixedSizeListVector) typed.vector).setNotNull(toIndex);
      if (getFlipAxisOrder()) {
        typed.writeOrdinal(y(toIndex), getCoordinateX(fromIndex));
        typed.writeOrdinal(x(toIndex), getCoordinateY(fromIndex));
      } else {
        typed.writeOrdinal(y(toIndex), getCoordinateY(fromIndex));
        typed.writeOrdinal(x(toIndex), getCoordinateX(fromIndex));
      }
    }
  }

  /**
   * Specialized read methods to return a single ordinate at a time. Does not check for null values.
   *
   * @param index index of the ordinate to read
   * @return y ordinate
   */
  public double getCoordinateY(int index) {
    return readOrdinal(y(index));

  }

  /**
   * Specialized read methods to return a single ordinate at a time. Does not check for null values.
   *
   * @param index index of the ordinate to read
   * @return x ordinate
   */
  public double getCoordinateX(int index) {
    return readOrdinal(x(index));
  }

  /**
   * Calculate the Y index
   */
  protected int y(int index) {
    return index * 2;
  }

  /**
   * Calculate the X index
   */
  protected int x(int index) {
    return index * 2 + 1;
  }
}
