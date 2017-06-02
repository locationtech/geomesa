/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import org.opengis.filter.expression.Literal

/**
 * Holder for a property name, literal value(s), and the order they are in
 */
case class PropertyLiteral(name: String,
                           literal: Literal,
                           secondary: Option[Literal],
                           flipped: Boolean = false)
