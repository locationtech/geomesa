/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector.floats;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.ValueVector.Accessor;
import org.apache.arrow.vector.ValueVector.Mutator;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.GeometryFields;
import org.locationtech.geomesa.arrow.vector.impl.AbstractPointVector;

import java.util.List;

public class PointFloatVector extends AbstractPointVector {

  // fields created by this vector
  public static final List<Field> fields = GeometryFields.XY_FLOAT;

  public PointFloatVector(String name, BufferAllocator allocator) {
    super(name, allocator);
  }

  public PointFloatVector(String name, AbstractContainerVector container) {
    super(name, container);
  }

  public PointFloatVector(FixedSizeListVector vector) {
    super(vector);
  }

  @Override
  protected List<Field> getFields() {
    return fields;
  }

  @Override
  protected PointWriter createWriter(FixedSizeListVector vector) {
    return new PointFloatWriter(vector);
  }

  @Override
  protected PointReader createReader(FixedSizeListVector vector) {
    return new PointFloatReader(vector);
  }

  public static class PointFloatWriter extends PointWriter {

    private NullableFloat4Vector.Mutator mutator;

    public PointFloatWriter(FixedSizeListVector vector) {
      super(vector);
    }

    @Override
    protected void setOrdinalMutator(Mutator mutator) {
      this.mutator = (NullableFloat4Vector.Mutator) mutator;
    }

    @Override
    protected void writeOrdinal(int index, double ordinal) {
      mutator.setSafe(index, (float) ordinal);
    }
  }

  public static class PointFloatReader extends PointReader {

    private NullableFloat4Vector.Accessor accessor;

    public PointFloatReader(FixedSizeListVector vector) {
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
