/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.transforms.EnrichmentCacheFunctionFactory.CacheLookup
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction

class EnrichmentCacheFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = Seq(cacheLookup)

  private val cacheLookup = new CacheLookup(null)
}

object EnrichmentCacheFunctionFactory {

  class CacheLookup(ec: EvaluationContext) extends NamedTransformerFunction(Seq("cacheLookup")) {

    override def apply(args: Array[AnyRef]): AnyRef = {
      val cache = ec.cache(args(0).asInstanceOf[String])
      cache.get(Array(args(1).asInstanceOf[String], args(2).asInstanceOf[String])).asInstanceOf[AnyRef]
<<<<<<< HEAD
<<<<<<< HEAD
=======
    }

<<<<<<< HEAD
    override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = {
      val cache = ec.cache(args(0).asInstanceOf[String])
      cache.get(Array(args(1).asInstanceOf[String], args(2).asInstanceOf[String]))
<<<<<<< HEAD
>>>>>>> 1ba2f23b3d (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> 1ba2f23b3 (GEOMESA-3071 Move all converter state into evaluation context)
>>>>>>> 74661c3147 (GEOMESA-3071 Move all converter state into evaluation context)
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
    }

=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
    override def withContext(ec: EvaluationContext): TransformerFunction = new CacheLookup(ec)
  }
}
