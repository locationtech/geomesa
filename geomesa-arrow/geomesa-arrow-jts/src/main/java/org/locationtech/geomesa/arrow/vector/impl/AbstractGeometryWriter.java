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
import org.locationtech.geomesa.arrow.vector.GeometryVector.GeometryWriter;

public abstract class AbstractGeometryWriter<T extends Geometry> implements GeometryWriter<T> {
  protected abstract void setOrdinalMutator(FieldVector.Mutator mutator);
  protected abstract void writeOrdinal(int index, double ordinal);
}
