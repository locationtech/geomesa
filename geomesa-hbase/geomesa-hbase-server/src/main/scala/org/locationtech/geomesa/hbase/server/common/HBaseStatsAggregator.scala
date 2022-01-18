/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.server.common

import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.iterators.StatsScan.StatResult

class HBaseStatsAggregator extends HBaseAggregator[StatResult] with StatsScan
