/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector.impl;

import org.locationtech.jts.geom.Geometry;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.GeometryVector;

import java.util.List;

public abstract class AbstractGeometryVector<T extends Geometry, U extends FieldVector, V extends FieldVector>
    implements GeometryVector<T, U> {

  private V ordinal;
  protected U vector;

  protected AbstractGeometryVector(U vector) {
    this.vector = vector;
  }

  @Override
  public U getVector() {
    return vector;
  }

  @Override
  public void transfer(int fromIndex, int toIndex, GeometryVector<T, U> to) {
    to.set(toIndex, get(fromIndex));
  }

  @Override
  public void setValueCount(int count) {
    vector.setValueCount(count);
  }

  @Override
  public int getValueCount() {
    return vector.getValueCount();
  }

  @Override
  public int getNullCount() {
    int count = vector.getNullCount();
    if (count < 0) {
      return 0;
    } else {
      return count;
    }
  }

  @Override
  public void close() throws Exception {
    vector.close();
  }

  protected void setOrdinalVector(V ordinal) {
    this.ordinal = ordinal;
  }

  protected V getOrdinalVector() {
    return ordinal;
  }

  protected abstract List<Field> getFields();
  protected abstract void writeOrdinal(int index, double ordinal);
  protected abstract double readOrdinal(int index);

  @Override
  @Deprecated
  public GeometryVector.GeometryWriter<T> getWriter() {
    return new GeometryWriter<T>() {
      @Override
      public void set(int i, T geom) {
        AbstractGeometryVector.this.set(i, geom);
      }
      @Override
      public void setValueCount(int count) {
        AbstractGeometryVector.this.setValueCount(count);
      }
    };
  }

  @Override
  @Deprecated
  public GeometryVector.GeometryReader<T> getReader() {
    return new GeometryReader<T>() {
      @Override
      public T get(int i) {
        return AbstractGeometryVector.this.get(i);

      }
      @Override
      public int getValueCount() {
        return AbstractGeometryVector.this.getValueCount();
      }
      @Override
      public int getNullCount() {
        return AbstractGeometryVector.this.getNullCount();
      }
    };
  }
}
