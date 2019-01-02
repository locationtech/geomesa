/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector.impl;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
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

public abstract class AbstractLineStringVector<T extends FieldVector>
    extends AbstractGeometryVector<LineString, ListVector, T> {

  public static FieldType createFieldType(Map<String, String> metadata) {
    return new FieldType(true, ArrowType.List.INSTANCE, null, metadata);
  }

  private final FixedSizeListVector tuples;

  protected AbstractLineStringVector(String name, BufferAllocator allocator, Map<String, String> metadata) {
    this(new ListVector(name, allocator, createFieldType(metadata), null));
  }

  protected AbstractLineStringVector(String name, AbstractContainerVector container, Map<String, String> metadata) {
    this(container.addOrGet(name, createFieldType(metadata), ListVector.class));
  }

  @SuppressWarnings("unchecked")
  protected AbstractLineStringVector(ListVector vector) {
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
  public void set(int index, LineString geom) {
    if (index == 0) {
      // need to do this to avoid issues with re-setting the value at index 0
      vector.setLastSet(0);
    }
    final int position = vector.startNewValue(index);
    if (geom == null) {
      vector.endValue(index, 0);
      BitVectorHelper.setValidityBit(vector.getValidityBuffer(), index, 0);
    } else {
      for (int i = 0; i < geom.getNumPoints(); i++) {
        final Coordinate p = geom.getCoordinateN(i);
        tuples.setNotNull(position + i);
        writeOrdinal((position + i) * 2, p.y);
        writeOrdinal((position + i) * 2 + 1, p.x);
      }
      vector.endValue(index, geom.getNumPoints());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public LineString get(int index) {
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
      return factory.createLineString(coordinates);
    }
  }

  public int getStartOffset(int index) {
    return vector.getOffsetBuffer().getInt(index * ListVector.OFFSET_WIDTH);
  }

  public int getEndOffset(int index) {
    return vector.getOffsetBuffer().getInt((index + 1) * ListVector.OFFSET_WIDTH);
  }

  public double getCoordinateY(int offset) {
    return readOrdinal(offset * 2);
  }

  public double getCoordinateX(int offset) {
    return readOrdinal(offset * 2 + 1);
  }
}
