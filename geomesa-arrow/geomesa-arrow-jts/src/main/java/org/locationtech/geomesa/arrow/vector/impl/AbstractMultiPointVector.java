/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector.impl;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Map;

public abstract class AbstractMultiPointVector<T extends FieldVector>
    extends AbstractGeometryVector<MultiPoint, ListVector, T> {

  public static FieldType createFieldType(Map<String, String> metadata) {
    return new FieldType(true, ArrowType.List.INSTANCE, null, metadata);
  }

  private final FixedSizeListVector tuples;

  protected AbstractMultiPointVector(String name, BufferAllocator allocator, Map<String, String> metadata) {
    this(new ListVector(name, allocator, createFieldType(metadata), null));
  }

  protected AbstractMultiPointVector(String name, AbstractContainerVector container, Map<String, String> metadata) {
    this(container.addOrGet(name, createFieldType(metadata), ListVector.class));
  }

  @SuppressWarnings("unchecked")
  protected AbstractMultiPointVector(ListVector vector) {
    super(vector);
    // create the fields we will write to up front
    if (vector.getDataVector().equals(BaseRepeatedValueVector.DEFAULT_DATA_VECTOR)) {
      vector.initializeChildrenFromFields(getFields());
      vector.allocateNew();
    }
    this.tuples = (FixedSizeListVector) vector.getChildrenFromFields().get(0);
    setOrdinalVector((T) tuples.getChildrenFromFields().get(0));
  }

  @Override
  public void set(int index, MultiPoint geom) {
    if (vector.getLastSet() >= index) {
      vector.setLastSet(index - 1);
    }
    final int position = vector.startNewValue(index);
    if (geom == null) {
      vector.endValue(index, 0);
      BitVectorHelper.setValidityBit(vector.getValidityBuffer(), index, 0);
    } else {
      for (int i = 0; i < geom.getNumPoints(); i++) {
        final Point p = (Point) geom.getGeometryN(i);
        tuples.setNotNull(position + i);
        writeOrdinal((position + i) * 2, p.getY());
        writeOrdinal((position + i) * 2 + 1, p.getX());
      }
      vector.endValue(index, geom.getNumPoints());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public MultiPoint get(int index) {
    if (vector.isNull(index)) {
      return null;
    } else {
      final int offsetStart = vector.getOffsetBuffer().getInt(index * ListVector.OFFSET_WIDTH);
      final int offsetEnd = vector.getOffsetBuffer().getInt((index + 1) * ListVector.OFFSET_WIDTH);
      final Coordinate[] coordinates = new Coordinate[offsetEnd - offsetStart];
      for (int i = 0; i < coordinates.length; i++) {
        final double y = readOrdinal((offsetStart + i) * 2);
        final double x = readOrdinal((offsetStart + i) * 2 + 1);
        coordinates[i] = new Coordinate(x, y);
      }
      return factory.createMultiPointFromCoords(coordinates);
    }
  }
}
