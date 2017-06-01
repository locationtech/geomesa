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
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.impl.AbstractPointVector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Double-precision vector for points
 */
public class PointVector extends AbstractPointVector {

  // fields created by this vector
  public static final List<Field> fields = GeometryFields.XY_DOUBLE;

  public PointVector(String name, BufferAllocator allocator, @Nullable Map<String, String> metadata) {
    super(name, allocator, metadata);
  }

  public PointVector(String name, AbstractContainerVector container, @Nullable Map<String, String> metadata) {
    super(name, container, metadata);
  }

  public PointVector(FixedSizeListVector vector) {
    super(vector);
  }

  @Override
  protected List<Field> getFields() {
    return fields;
  }

  @Override
  protected PointWriter createWriter(FixedSizeListVector vector) {
    return new PointDoubleWriter(vector);
  }

  @Override
  protected PointReader createReader(FixedSizeListVector vector) {
    return new PointDoubleReader(vector);
  }

  public static class PointDoubleWriter extends PointWriter {

    private NullableFloat8Vector.Mutator mutator;

    public PointDoubleWriter(FixedSizeListVector vector) {
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

  public static class PointDoubleReader extends PointReader {

    private NullableFloat8Vector.Accessor accessor;

    public PointDoubleReader(FixedSizeListVector vector) {
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
