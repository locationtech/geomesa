/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter.visitor

import org.geotools.api.filter.Id
import org.geotools.filter.visitor.DefaultFilterVisitor

/**
  * Returns true if filter contains an ID filter
  */
class IdDetectingFilterVisitor extends DefaultFilterVisitor {
  override def visit(f: Id, data: AnyRef): AnyRef = java.lang.Boolean.TRUE
}
