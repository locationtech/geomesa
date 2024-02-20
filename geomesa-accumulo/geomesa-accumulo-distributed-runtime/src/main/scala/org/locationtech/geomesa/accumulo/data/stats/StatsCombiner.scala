/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

/**
 * The class is kept here for back-compatibility on already configured tables. Note that it violates
 * split-packaging with accumulo-datastore, but this module only builds a shaded jar so packages get
 * flattened out.
 */
@deprecated("Moved to org.locationtech.geomesa.accumulo.combiners.StatsCombiner")
class StatsCombiner extends org.locationtech.geomesa.accumulo.combiners.StatsCombiner
