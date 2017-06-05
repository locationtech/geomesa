/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector.impl;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListReader;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.locationtech.geomesa.arrow.vector.GeometryVector;

import java.util.List;

public abstract class AbstractMultiPolygonVector implements GeometryVector<MultiPolygon, ListVector> {

  private final ListVector vector;
  private final MultiPolygonWriter writer;
  private final MultiPolygonReader reader;

  protected AbstractMultiPolygonVector(String name, BufferAllocator allocator) {
    this(new ListVector(name, allocator, null, null));
  }

  protected AbstractMultiPolygonVector(String name, AbstractContainerVector container) {
    this(container.addOrGet(name, new FieldType(true, ArrowType.List.INSTANCE, null), ListVector.class));
  }

  protected AbstractMultiPolygonVector(ListVector vector) {
    this.vector = vector;
    // create the fields we will write to up front
    if (vector.getDataVector().equals(BaseRepeatedValueVector.DEFAULT_DATA_VECTOR)) {
      vector.initializeChildrenFromFields(getFields());
      this.vector.allocateNew();
    }
    this.writer = createWriter(vector);
    this.reader = createReader(vector);
  }

  protected abstract List<Field> getFields();
  protected abstract MultiPolygonWriter createWriter(ListVector vector);
  protected abstract MultiPolygonReader createReader(ListVector vector);

  @Override
  public MultiPolygonWriter getWriter() {
    return writer;
  }

  @Override
  public MultiPolygonReader getReader() {
    return reader;
  }

  @Override
  public ListVector getVector() {
    return vector;
  }

  @Override
  public void close() throws Exception {
    vector.close();
  }

  public static abstract class MultiPolygonWriter implements GeometryWriter<MultiPolygon> {

    private final ListVector.Mutator mutator;
    private final ListVector.Mutator innerMutator;
    private final ListVector.Mutator innerInnerMutator;
    private final FixedSizeListVector.Mutator tupleMutator;
    private final FieldVector.Mutator pointMutator;

    protected MultiPolygonWriter(ListVector vector) {
      ListVector innerList = (ListVector) vector.getChildrenFromFields().get(0);
      ListVector innerInnerList = (ListVector) innerList.getChildrenFromFields().get(0);
      FixedSizeListVector tuples = (FixedSizeListVector) innerInnerList.getChildrenFromFields().get(0);
      this.mutator = vector.getMutator();
      this.innerMutator = innerList.getMutator();
      this.innerInnerMutator = innerInnerList.getMutator();
      this.tupleMutator = tuples.getMutator();
      this.pointMutator = tuples.getChildrenFromFields().get(0).getMutator();
    }

    @Override
    public void set(int index, MultiPolygon geom) {
      if (geom != null) {
        int innerIndex = mutator.startNewValue(index);
        for (int i = 0; i < geom.getNumGeometries(); i++) {
          Polygon poly = (Polygon) geom.getGeometryN(i);
          int innerInnerIndex = innerMutator.startNewValue(innerIndex + i);
          for (int j = 0; j < poly.getNumInteriorRing() + 1; j++) {
            LineString line = j == 0 ? poly.getExteriorRing() : poly.getInteriorRingN(j - 1);
            int position = innerInnerMutator.startNewValue(innerInnerIndex + j);
            for (int k = 0; k < line.getNumPoints(); k++) {
              Coordinate p = line.getCoordinateN(k);
              tupleMutator.setNotNull(position + k);
              writeOrdinal(pointMutator, (position + k) * 2, p.y);
              writeOrdinal(pointMutator, (position + k) * 2 + 1, p.x);
            }
            innerInnerMutator.endValue(innerInnerIndex + j, line.getNumPoints());
          }
          innerMutator.endValue(innerIndex + i, poly.getNumInteriorRing() + 1);
        }
        mutator.endValue(index, geom.getNumGeometries());
      }
    }

    protected abstract void writeOrdinal(FieldVector.Mutator mutator, int index, double ordinal);

    @Override
    public void setValueCount(int count) {
      mutator.setValueCount(count);
    }
  }

  public static abstract class MultiPolygonReader implements GeometryReader<MultiPolygon> {

    private final ListVector.Accessor accessor;
    private final UnionListReader reader;
    private final UnionListReader innerReader;
    private final UnionListReader innerInnerReader;
    private final UnionFixedSizeListReader innerInnerInnerReader;
    private final FieldReader ordinalReader;

    public MultiPolygonReader(ListVector vector) {
      ListVector innerVector = (ListVector) vector.getChildrenFromFields().get(0);
      ListVector innerInnerVector = (ListVector) innerVector.getChildrenFromFields().get(0);
      this.accessor = vector.getAccessor();
      this.reader = vector.getReader();
      this.innerReader = innerVector.getReader();
      this.innerInnerReader = innerInnerVector.getReader();
      this.innerInnerInnerReader = ((FixedSizeListVector) innerInnerVector.getChildrenFromFields().get(0)).getReader();
      this.ordinalReader = innerInnerInnerReader.reader();
    }

    @Override
    @SuppressWarnings("unchecked")
    public MultiPolygon get(int index) {
      reader.setPosition(index);
      if (reader.isSet()) {
        Polygon[] polygons = new Polygon[reader.size()];
        for (int i = 0; i < polygons.length; i++) {
          reader.next();
          LinearRing shell = null;
          LinearRing[] holes = new LinearRing[innerReader.size() - 1];
          for (int j = 0; j < holes.length + 1; j++) {
            innerReader.next();
            Coordinate[] coordinates = new Coordinate[innerInnerReader.size()];
            for (int k = 0; k < coordinates.length; k++) {
              innerInnerReader.next();
              innerInnerInnerReader.next();
              double y = readOrdinal(ordinalReader);
              innerInnerInnerReader.next();
              double x = readOrdinal(ordinalReader);
              coordinates[k] = new Coordinate(x, y);
            }
            LinearRing ring = factory.createLinearRing(coordinates);
            if (j == 0) {
              shell = ring;
            } else {
              holes[j - 1] = ring;
            }
          }
          polygons[i] = factory.createPolygon(shell, holes);
        }
        return factory.createMultiPolygon(polygons);
      } else {
        return null;
      }
    }

    protected abstract double readOrdinal(FieldReader reader);

    @Override
    public int getValueCount() {
      return accessor.getValueCount();
    }

    @Override
    public int getNullCount() {
      return accessor.getNullCount();
    }
  }
}
