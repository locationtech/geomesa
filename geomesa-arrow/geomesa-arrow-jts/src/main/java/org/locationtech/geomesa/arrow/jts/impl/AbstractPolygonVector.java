/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.jts.impl;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

import java.util.Map;

public abstract class AbstractPolygonVector<T extends FieldVector>
    extends AbstractGeometryVector<Polygon, ListVector, T> {

  public static FieldType createFieldType(Map<String, String> metadata) {
    return new FieldType(true, ArrowType.List.INSTANCE, null, metadata);
  }

  private final ListVector innerVector;
  private final FixedSizeListVector tuples;

  protected AbstractPolygonVector(String name, BufferAllocator allocator, Map<String, String> metadata) {
    this(new ListVector(name, allocator, createFieldType(metadata), null));
  }

  protected AbstractPolygonVector(String name, AbstractContainerVector container, Map<String, String> metadata) {
    this(container.addOrGet(name, createFieldType(metadata), ListVector.class));
  }

  @SuppressWarnings("unchecked")
  protected AbstractPolygonVector(ListVector vector) {
    super(vector);
    // create the fields we will write to up front
    if (vector.getDataVector().equals(BaseRepeatedValueVector.DEFAULT_DATA_VECTOR)) {
      vector.initializeChildrenFromFields(getFields());
      vector.allocateNew();
    }
    this.innerVector = (ListVector) vector.getChildrenFromFields().get(0);
    this.tuples = (FixedSizeListVector) innerVector.getChildrenFromFields().get(0);
    setOrdinalVector((T) tuples.getChildrenFromFields().get(0));
  }

  @Override
  public void set(int index, Polygon geom) {
    if (vector.getLastSet() >= index) {
      vector.setLastSet(index - 1);
      innerVector.setLastSet(index - 1);
    }
    final int innerIndex = vector.startNewValue(index);
    if (geom == null) {
      vector.endValue(index, 0);
      BitVectorHelper.setValidityBit(vector.getValidityBuffer(), index, 0);
    } else {
      for (int i = 0; i < geom.getNumInteriorRing() + 1; i++) {
        final LineString line = i == 0 ? geom.getExteriorRing() : geom.getInteriorRingN(i - 1);
        final int position = innerVector.startNewValue(innerIndex + i);
        for (int j = 0; j < line.getNumPoints(); j++) {
          final Coordinate p = line.getCoordinateN(j);
          tuples.setNotNull(position + j);
          writeOrdinal((position + j) * 2, p.y);
          writeOrdinal((position + j) * 2 + 1, p.x);
        }
        innerVector.endValue(innerIndex + i, line.getNumPoints());
      }
      vector.endValue(index, geom.getNumInteriorRing() + 1);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Polygon get(int index) {
    if (vector.isNull(index)) {
      return null;
    } else {
      final int outerOffsetStart = vector.getOffsetBuffer().getInt(index * ListVector.OFFSET_WIDTH);
      final int outerOffsetEnd = vector.getOffsetBuffer().getInt((index + 1) * ListVector.OFFSET_WIDTH);
      LinearRing shell = null;
      final LinearRing[] holes = new LinearRing[outerOffsetEnd - outerOffsetStart - 1];
      for (int j = 0; j < holes.length + 1; j++) {
        final int offsetStart = innerVector.getOffsetBuffer().getInt((outerOffsetStart + j) * ListVector.OFFSET_WIDTH);
        final int offsetEnd = innerVector.getOffsetBuffer().getInt((outerOffsetStart + j + 1) * ListVector.OFFSET_WIDTH);
        final Coordinate[] coordinates = new Coordinate[offsetEnd - offsetStart];
        for (int i = 0; i < coordinates.length; i++) {
          final double y = readOrdinal((offsetStart + i) * 2);
          final double x = readOrdinal((offsetStart + i) * 2 + 1);
          coordinates[i] = new Coordinate(x, y);
        }
        final LinearRing ring = factory.createLinearRing(coordinates);
        if (j == 0) {
          shell = ring;
        } else {
          holes[j - 1] = ring;
        }
      }
      return factory.createPolygon(shell, holes);
    }
  }
}
