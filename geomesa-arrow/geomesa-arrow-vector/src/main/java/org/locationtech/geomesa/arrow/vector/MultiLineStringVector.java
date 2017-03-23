/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.MultiLineString;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.locationtech.geomesa.arrow.vector.reader.MultiLineStringReader;
import org.locationtech.geomesa.arrow.vector.writer.MultiLineStringWriter;

public class MultiLineStringVector implements GeometryVector<MultiLineString> {

  private final NullableMapVector vector;
  private final MultiLineStringWriter writer;
  private final MultiLineStringReader reader;

  public MultiLineStringVector(String name, BufferAllocator allocator) {
    this(new NullableMapVector(name, allocator, null, null));
    this.vector.allocateNew();
  }

  public MultiLineStringVector(NullableMapVector vector) {
    this.vector = vector;
    vector.addOrGet("x", MinorType.LIST, ListVector.class, null);
    vector.addOrGet("y", MinorType.LIST, ListVector.class, null);
    this.writer = new MultiLineStringWriter(new NullableMapWriter(vector));
    this.reader = new MultiLineStringReader(vector);
  }

  @Override
  public MultiLineStringWriter getWriter() {
    return writer;
  }

  @Override
  public MultiLineStringReader getReader() {
    return reader;
  }

  @Override
  public NullableMapVector getVector() {
    return vector;
  }

  @Override
  public void close() throws Exception {
    writer.close();
    reader.close();
    vector.close();
  }
}
