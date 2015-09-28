/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.filter.visitor

import org.opengis.feature.simple.SimpleFeatureType

class QueryPlanFilterVisitor(val sft: SimpleFeatureType)
    extends LocalNameVisitor with SafeTopologicalFilterVisitor
