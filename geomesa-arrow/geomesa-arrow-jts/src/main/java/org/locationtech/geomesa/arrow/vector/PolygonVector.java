/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.ValueVector.Accessor;
import org.apache.arrow.vector.ValueVector.Mutator;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.impl.AbstractPolygonVector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Double-precision vector for polygons
 */
public class PolygonVector extends AbstractPolygonVector {

  // fields created by this vector
  public static final List<Field> fields = GeometryFields.XY_DOUBLE_LIST_2;

  public PolygonVector(String name, BufferAllocator allocator, @Nullable Map<String, String> metadata) {
    super(name, allocator, metadata);
  }

  public PolygonVector(String name, AbstractContainerVector container, @Nullable Map<String, String> metadata) {
    super(name, container, metadata);
  }

  public PolygonVector(ListVector vector) {
    super(vector);
  }

  @Override
  protected List<Field> getFields() {
    return fields;
  }

  @Override
  protected PolygonWriter createWriter(ListVector vector) {
    return new PolygonDoubleWriter(vector);
  }

  @Override
  protected PolygonReader createReader(ListVector vector) {
    return new PolygonDoubleReader(vector);
  }

  public static class PolygonDoubleWriter extends PolygonWriter {

    private NullableFloat8Vector.Mutator mutator;

    public PolygonDoubleWriter(ListVector vector) {
      super(vector);
    }

    @Override
    protected void setOrdinalMutator(Mutator mutator) {
      this.mutator = (NullableFloat8Vector.Mutator) mutator;
    }

    @Override
    protected void writeOrdinal(int index, double ordinal) {
      mutator.set(index, ordinal);
    }
  }

  public static class PolygonDoubleReader extends PolygonReader {

    private NullableFloat8Vector.Accessor accessor;

    public PolygonDoubleReader(ListVector vector) {
      super(vector);
    }

    @Override
    protected void setOrdinalAccessor(Accessor accessor) {
      this.accessor = (NullableFloat8Vector.Accessor) accessor;
    }

    @Override
    protected double readOrdinal(int index) {
      return accessor.get(index);
    }
  }
}
