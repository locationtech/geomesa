/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.ValueVector.Accessor;
import org.apache.arrow.vector.ValueVector.Mutator;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.GeometryFields;
import org.locationtech.geomesa.arrow.vector.impl.AbstractMultiPolygonVector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class MultiPolygonFloatVector extends AbstractMultiPolygonVector {

  // fields created by this vector
  public static final List<Field> fields = GeometryFields.XY_FLOAT_LIST_3;

  public MultiPolygonFloatVector(String name, BufferAllocator allocator, @Nullable Map<String, String> metadata) {
    super(name, allocator, metadata);
  }

  public MultiPolygonFloatVector(String name, AbstractContainerVector container, @Nullable Map<String, String> metadata) {
    super(name, container, metadata);
  }

  public MultiPolygonFloatVector(ListVector vector) {
    super(vector);
  }

  @Override
  protected List<Field> getFields() {
    return fields;
  }

  @Override
  protected MultiPolygonWriter createWriter(ListVector vector) {
    return new MultiPolygonFloatWriter(vector);
  }

  @Override
  protected MultiPolygonReader createReader(ListVector vector) {
    return new MultiPolygonFloatReader(vector);
  }

  public static class MultiPolygonFloatWriter extends MultiPolygonWriter {

    private NullableFloat4Vector.Mutator mutator;

    public MultiPolygonFloatWriter(ListVector vector) {
      super(vector);
    }

    @Override
    protected void setOrdinalMutator(Mutator mutator) {
      this.mutator = (NullableFloat4Vector.Mutator) mutator;
    }

    @Override
    protected void writeOrdinal(int index, double ordinal) {
      mutator.set(index, (float) ordinal);
    }
  }

  public static class MultiPolygonFloatReader extends MultiPolygonReader {

    private NullableFloat4Vector.Accessor accessor;

    public MultiPolygonFloatReader(ListVector vector) {
      super(vector);
    }

    @Override
    protected void setOrdinalAccessor(Accessor accessor) {
      this.accessor = (NullableFloat4Vector.Accessor) accessor;
    }

    @Override
    protected double readOrdinal(int index) {
      return accessor.get(index);
    }
  }
}
