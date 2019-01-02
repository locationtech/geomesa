/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.impl.AbstractMultiLineStringVector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Double-precision vector for multi-line strings
 */
public class MultiLineStringVector extends AbstractMultiLineStringVector<Float8Vector> {

  // fields created by this vector
  public static final List<Field> fields = GeometryFields.XY_DOUBLE_LIST_2;

  public MultiLineStringVector(String name, BufferAllocator allocator, @Nullable Map<String, String> metadata) {
    super(name, allocator, metadata);
  }

  public MultiLineStringVector(String name, AbstractContainerVector container, @Nullable Map<String, String> metadata) {
    super(name, container, metadata);
  }

  public MultiLineStringVector(ListVector vector) {
    super(vector);
  }

  @Override
  protected List<Field> getFields() {
    return fields;
  }

  @Override
  protected void writeOrdinal(int index, double ordinal) {
    getOrdinalVector().setSafe(index, ordinal);
  }

  @Override
  protected double readOrdinal(int index) {
    return getOrdinalVector().get(index);
  }
}
