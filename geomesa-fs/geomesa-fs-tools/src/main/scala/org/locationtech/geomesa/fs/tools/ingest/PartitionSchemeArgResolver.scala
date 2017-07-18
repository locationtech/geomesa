/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.tools.ingest

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory
import org.locationtech.geomesa.fs.storage.api.PartitionScheme
import org.locationtech.geomesa.fs.storage.common.CommonSchemeLoader
import org.locationtech.geomesa.utils.classpath.PathUtils
import org.locationtech.geomesa.utils.conf.ArgResolver
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal


object PartitionSchemeArgResolver extends ArgResolver[PartitionScheme, SchemeArgs] {
  import org.locationtech.geomesa.utils.conf.ArgResolver.ArgTypes._

  private val fileNameReg = """([^.]*)\.([^.]*)""" // e.g. "foo.bar"
  private val confStrings = Seq("{")

  override protected def argType(args: SchemeArgs): ArgTypes = {
    // guess the type we are trying to parse, to determine which error we show for failures
    // order is important here
    if (confStrings.exists(args.scheme.contains)) {
      CONFSTR
    } else if (args.scheme.matches(fileNameReg) || args.scheme.contains("/")) {
      PATH
    } else {
      NAME
    }
  }

  override val parseMethodList: Seq[(SchemeArgs) => ResEither] = List[SchemeArgs => ResEither](
    getNamedScheme,
    parseFile,
    parseString
  )

  private [PartitionSchemeArgResolver] def getNamedScheme(args: SchemeArgs): ResEither = {
    try {
      Right(CommonSchemeLoader.build(args.scheme, args.sft))
    } catch {
      case NonFatal(e) => Left((s"Unable to load named scheme ${args.scheme}", e, NAME))
    }
  }

  private [PartitionSchemeArgResolver] def parseString(args: SchemeArgs): ResEither = {
    try {
      Right(org.locationtech.geomesa.fs.storage.common.PartitionScheme.apply(args.sft, args.scheme))
    } catch {
      case NonFatal(e) => Left((s"Unable to load scheme from arg ${args.scheme}", e, CONFSTR))
    }
  }

  private [PartitionSchemeArgResolver] def parseFile(args: SchemeArgs): ResEither = {
    try {
      val reader = new InputStreamReader(PathUtils.getInputStream(args.scheme), StandardCharsets.UTF_8)
      val conf = ConfigFactory.parseReader(reader, parseOpts)
      Right(org.locationtech.geomesa.fs.storage.common.PartitionScheme.apply(args.sft, conf))
    } catch {
      case NonFatal(e) => Left((s"Unable to load scheme from file ${args.scheme}", e, PATH))
    }
  }
}

case class SchemeArgs(scheme: String, sft: SimpleFeatureType)