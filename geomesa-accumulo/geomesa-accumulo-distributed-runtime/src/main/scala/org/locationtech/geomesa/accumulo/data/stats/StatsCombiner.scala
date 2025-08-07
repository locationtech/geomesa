/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

/**
 * The class is kept here for back-compatibility on already configured tables. Note that it violates
 * split-packaging with accumulo-datastore, but this module only builds a shaded jar so packages get
 * flattened out.
 */
@deprecated("Moved to org.locationtech.geomesa.accumulo.combiners.StatsCombiner")
class StatsCombiner extends org.locationtech.geomesa.accumulo.combiners.StatsCombiner
