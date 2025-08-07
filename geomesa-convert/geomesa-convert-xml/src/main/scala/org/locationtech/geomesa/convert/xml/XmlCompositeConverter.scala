/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert.xml

import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.Modes.{ErrorMode, LineMode}
import org.locationtech.geomesa.convert.xml.XmlConverter.DocParser
import org.locationtech.geomesa.convert2.transforms.Predicate
import org.locationtech.geomesa.convert2.{AbstractCompositeConverter, ParsingConverter}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.w3c.dom.Element

import java.io.InputStream
import java.nio.charset.Charset

class XmlCompositeConverter(
    sft: SimpleFeatureType,
    xsd: Option[String],
    encoding: Charset,
    lineMode: LineMode,
    errorMode: ErrorMode,
    delegates: Seq[(Predicate, ParsingConverter[Element])]
  ) extends AbstractCompositeConverter[Element](sft, errorMode, delegates) {

  private val parser = new ThreadLocal[DocParser]() {
    override def initialValue(): DocParser = new DocParser(xsd)
  }

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[Element] =
    XmlConverter.iterator(parser.get, is, encoding, lineMode, ec)
}
