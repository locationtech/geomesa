/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api;

import org.opengis.feature.simple.SimpleFeature;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public interface FileSystemReader extends Iterator<SimpleFeature>, AutoCloseable {
  boolean close(long wait, TimeUnit unit);
}
