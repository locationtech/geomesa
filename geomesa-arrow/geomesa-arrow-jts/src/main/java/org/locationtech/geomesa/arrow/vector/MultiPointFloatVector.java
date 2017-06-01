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
import org.locationtech.geomesa.arrow.vector.impl.AbstractMultiPointVector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class MultiPointFloatVector extends AbstractMultiPointVector {

  // fields created by this vector
  public static final List<Field> fields = GeometryFields.XY_FLOAT_LIST;

  public MultiPointFloatVector(String name, BufferAllocator allocator,@Nullable Map<String, String> metadata) {
    super(name, allocator, metadata);
  }

  public MultiPointFloatVector(String name, AbstractContainerVector container, @Nullable Map<String, String> metadata) {
    super(name, container, metadata);
  }

  public MultiPointFloatVector(ListVector vector) {
    super(vector);
  }

  @Override
  protected List<Field> getFields() {
    return fields;
  }

  @Override
  protected MultiPointWriter createWriter(ListVector vector) {
    return new MultiPointFloatWriter(vector);
  }

  @Override
  protected MultiPointReader createReader(ListVector vector) {
    return new MultiPointFloatReader(vector);
  }

  public static class MultiPointFloatWriter extends MultiPointWriter {

    private NullableFloat4Vector.Mutator mutator;

    public MultiPointFloatWriter(ListVector vector) {
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

  public static class MultiPointFloatReader extends MultiPointReader {

    private NullableFloat4Vector.Accessor accessor;

    public MultiPointFloatReader(ListVector vector) {
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
