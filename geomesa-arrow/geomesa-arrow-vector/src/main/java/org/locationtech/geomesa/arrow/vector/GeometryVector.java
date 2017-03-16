/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.locationtech.geomesa.arrow.vector.reader.GeometryReader;
import org.locationtech.geomesa.arrow.vector.writer.GeometryWriter;

public interface GeometryVector<T extends Geometry> extends AutoCloseable {

  public GeometryWriter<T> getWriter();
  public GeometryReader<T> getReader();
  public NullableMapVector getVector();
}
