/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector.impl;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.arrow.vector.FieldVector;
import org.locationtech.geomesa.arrow.vector.GeometryVector.GeometryReader;
import org.locationtech.geomesa.arrow.vector.GeometryVector.GeometryWriter;

public abstract class AbstractGeometryReader<T extends Geometry> implements GeometryReader<T> {
  protected abstract void setOrdinalAccessor(FieldVector.Accessor accessor);
  protected abstract double readOrdinal(int index);
}
