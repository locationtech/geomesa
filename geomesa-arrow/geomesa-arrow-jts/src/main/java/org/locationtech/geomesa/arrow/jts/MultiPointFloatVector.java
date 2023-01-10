/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.jts;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.jts.impl.AbstractMultiPointVector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class MultiPointFloatVector extends AbstractMultiPointVector<Float4Vector>  {

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
  protected void writeOrdinal(int index, double ordinal) {
    getOrdinalVector().setSafe(index, (float) ordinal);
  }

  @Override
  protected double readOrdinal(int index) {
    return getOrdinalVector().get(index);
  }
}
