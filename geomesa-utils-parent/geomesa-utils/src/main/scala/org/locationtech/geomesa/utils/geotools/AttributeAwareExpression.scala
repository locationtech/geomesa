/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.filter.FilterAttributeExtractor

/**
 * Marker trait for filter expressions that have special handling for extracting affected attributes
 */
trait AttributeAwareExpression {

  /**
   * Extract attributes and pass them on to the visitor
   *
   * @param visitor extracting visitor
   * @param data extra data
   * @return extra data
   */
  def visit(visitor: FilterAttributeExtractor, data: AnyRef): AnyRef
}
