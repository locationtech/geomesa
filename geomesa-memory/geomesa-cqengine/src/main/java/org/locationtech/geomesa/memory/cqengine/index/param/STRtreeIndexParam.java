/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.memory.cqengine.index.param;

import org.locationtech.geomesa.memory.cqengine.index.GeoIndexType;

public class STRtreeIndexParam implements GeoIndexParams {

    int nodeCapacity = 10;

    public STRtreeIndexParam() {
    }

    public STRtreeIndexParam(int nodeCapacity) {
        this.nodeCapacity = nodeCapacity;
    }

    public int getNodeCapacity() {
        return nodeCapacity;
    }

    @Override
    public GeoIndexType getGeoIndexType() {
        return GeoIndexType.STRtree;
    }
}
