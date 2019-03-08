/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.xml

import java.io.InputStream
import java.nio.charset.Charset

import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.Modes.{ErrorMode, LineMode}
import org.locationtech.geomesa.convert.xml.XmlConverter.DocParser
import org.locationtech.geomesa.convert2.transforms.Predicate
import org.locationtech.geomesa.convert2.{AbstractCompositeConverter, ParsingConverter}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeatureType
import org.w3c.dom.Element

class XmlCompositeConverter(
    sft: SimpleFeatureType,
    xsd: Option[String],
    encoding: Charset,
    lineMode: LineMode,
    errorMode: ErrorMode,
    delegates: Seq[(Predicate, ParsingConverter[Element])]
  ) extends AbstractCompositeConverter[Element](sft, errorMode, delegates) {

  private val parser = new DocParser(xsd)

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[Element] =
    XmlConverter.iterator(parser, is, encoding, lineMode, ec.counter)
}
