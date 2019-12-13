/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet.jobs

import org.locationtech.geomesa.fs.storage.common.jobs.PartitionOutputFormat

class ParquetPartitionOutputFormat extends PartitionOutputFormat(new ParquetSimpleFeatureOutputFormat())
