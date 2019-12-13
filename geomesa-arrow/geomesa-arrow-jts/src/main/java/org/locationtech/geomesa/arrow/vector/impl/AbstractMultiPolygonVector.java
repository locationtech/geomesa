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
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
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

public abstract class AbstractMultiPolygonVector<T extends FieldVector>
    extends AbstractGeometryVector<MultiPolygon, ListVector, T> {

  public static FieldType createFieldType(Map<String, String> metadata) {
    return new FieldType(true, ArrowType.List.INSTANCE, null, metadata);
  }

  private final ListVector innerVector;
  private final ListVector innerInnerVector;
  private final FixedSizeListVector tuples;

  protected AbstractMultiPolygonVector(String name, BufferAllocator allocator, Map<String, String> metadata) {
    this(new ListVector(name, allocator, createFieldType(metadata), null));
  }

  protected AbstractMultiPolygonVector(String name, AbstractContainerVector container, Map<String, String> metadata) {
    this(container.addOrGet(name, createFieldType(metadata), ListVector.class));
  }

  @SuppressWarnings("unchecked")
  protected AbstractMultiPolygonVector(ListVector vector) {
    super(vector);
    // create the fields we will write to up front
    if (vector.getDataVector().equals(BaseRepeatedValueVector.DEFAULT_DATA_VECTOR)) {
      vector.initializeChildrenFromFields(getFields());
      vector.allocateNew();
    }
    this.innerVector = (ListVector) vector.getChildrenFromFields().get(0);
    this.innerInnerVector = (ListVector) innerVector.getChildrenFromFields().get(0);
    this.tuples = (FixedSizeListVector) innerInnerVector.getChildrenFromFields().get(0);
    setOrdinalVector((T) tuples.getChildrenFromFields().get(0));
  }

  @Override
  public void set(int index, MultiPolygon geom) {
    if (index == 0) {
      // need to do this to avoid issues with re-setting the value at index 0
      vector.setLastSet(0);
      innerVector.setLastSet(0);
      innerInnerVector.setLastSet(0);
    }
    int innerIndex = vector.startNewValue(index);
    if (geom == null) {
      vector.endValue(index, 0);
      BitVectorHelper.setValidityBit(vector.getValidityBuffer(), index, 0);
    } else {
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        Polygon poly = (Polygon) geom.getGeometryN(i);
        int innerInnerIndex = innerVector.startNewValue(innerIndex + i);
        for (int j = 0; j < poly.getNumInteriorRing() + 1; j++) {
          LineString line = j == 0 ? poly.getExteriorRing() : poly.getInteriorRingN(j - 1);
          int position = innerInnerVector.startNewValue(innerInnerIndex + j);
          for (int k = 0; k < line.getNumPoints(); k++) {
            Coordinate p = line.getCoordinateN(k);
            tuples.setNotNull(position + k);
            writeOrdinal((position + k) * 2, p.y);
            writeOrdinal((position + k) * 2 + 1, p.x);
          }
          innerInnerVector.endValue(innerInnerIndex + j, line.getNumPoints());
        }
        innerVector.endValue(innerIndex + i, poly.getNumInteriorRing() + 1);
      }
      vector.endValue(index, geom.getNumGeometries());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public MultiPolygon get(int index) {
    if (vector.isNull(index)) {
      return null;
    } else {
      final int outerOuterOffsetStart = vector.getOffsetBuffer().getInt(index * ListVector.OFFSET_WIDTH);
      final int outerOuterOffsetEnd = vector.getOffsetBuffer().getInt((index + 1) * ListVector.OFFSET_WIDTH);
      final Polygon[] polygons = new Polygon[outerOuterOffsetEnd - outerOuterOffsetStart];
      for (int k = 0; k < polygons.length; k++) {
        final int outerOffsetStart = innerVector.getOffsetBuffer().getInt((outerOuterOffsetStart + k) * ListVector.OFFSET_WIDTH);
        final int outerOffsetEnd = innerVector.getOffsetBuffer().getInt((outerOuterOffsetStart + k + 1) * ListVector.OFFSET_WIDTH);
        LinearRing shell = null;
        final LinearRing[] holes = new LinearRing[outerOffsetEnd - outerOffsetStart - 1];
        for (int j = 0; j < holes.length + 1; j++) {
          final int offsetStart = innerInnerVector.getOffsetBuffer().getInt((outerOffsetStart + j) * ListVector.OFFSET_WIDTH);
          final int offsetEnd = innerInnerVector.getOffsetBuffer().getInt((outerOffsetStart + j + 1) * ListVector.OFFSET_WIDTH);
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
        polygons[k] = factory.createPolygon(shell, holes);
      }
      return factory.createMultiPolygon(polygons);
    }
  }
}
