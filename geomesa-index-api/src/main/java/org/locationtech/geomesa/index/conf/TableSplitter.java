/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf;

import org.opengis.feature.simple.SimpleFeatureType;

public interface TableSplitter {
    byte[][] getSplits(SimpleFeatureType sft, String index, String options);
}
