/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.convert.scripting

import java.io.File
import java.net.URI
import javax.script.{Invocable, ScriptContext, ScriptEngine, ScriptEngineManager}

import com.google.common.io.Files
import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.commons.io.{FileUtils, IOUtils}
import org.locationtech.geomesa.convert.Transformers.EvaluationContext
import org.locationtech.geomesa.convert.{TransformerFn, TransformerFunctionFactory}

import scala.collection.JavaConversions._

/**
  * Provides TransformerFunctions that execute javax.scripts compatible functions defined
  * 'classpath:geomesa-convert-scripts'.  Scripting languages are determined by the extension
  * of the file.  For instance, 'name.js' will be interpreted as a javascript file.
  */
class ScriptingFunctionFactory extends TransformerFunctionFactory {

  val SCRIPT_PATH = "geomesa-convert-scripts/"
  val SCRIPT_PATH_ENV_VAR = "geomesa.convert.scripts.path"

  lazy val functions = init()

  private def init() = {
    val loader = Thread.currentThread().getContextClassLoader
    val scriptfiles = getScriptsFromEnvVar ++ getScriptsFromClasspath(loader)

    val mgr = new ScriptEngineManager()
    scriptfiles
      .map { f => (f, Files.getFileExtension(f.getPath)) }
      .groupBy { case (_, ext) => ext }
      .flatMap { case (ext, files) =>
        val engine = Option(mgr.getEngineByExtension(ext))
        engine.map { e => evaluateScriptsForEngine(loader, files, e, ext) }.getOrElse(Seq())
      }.toList
  }

  def getScriptsFromClasspath(loader: ClassLoader): Seq[URI] = {
    Option(loader.getResourceAsStream(SCRIPT_PATH)).map { scripts =>
      IOUtils
        .readLines(scripts, "UTF-8")
        .map { s => loader.getResource(s"$SCRIPT_PATH/$s").toURI }
    }.getOrElse(Seq.empty[URI])
  }

  def getScriptsFromEnvVar: Seq[URI] = {
    val v = System.getProperty(SCRIPT_PATH_ENV_VAR)
    if (v == null) Seq.empty[URI]
    else {
      v.split(",")
        .map { d => new File(d) }
        .filter { f => f.exists() && f.isDirectory }
        .flatMap { f =>
          FileUtils.listFiles(f, TrueFileFilter.TRUE, TrueFileFilter.TRUE).map(_.toURI)
        }
    }
  }

  private def evaluateScriptsForEngine(loader: ClassLoader, files: Seq[(URI, String)], e: ScriptEngine, ext: String) = {
    files.foreach { case (f, _) => evalScriptFile(loader, e, f) }
    e.getBindings(ScriptContext.ENGINE_SCOPE).map { case (k, v) =>
      new ScriptTransformerFn(ext, k, e.asInstanceOf[Invocable])
    }
  }

  def evalScriptFile(loader: ClassLoader, e: ScriptEngine, f: URI): AnyRef =
    try {
      val lines = IOUtils.toString(f, "UTF-8")
      e.eval(lines)
    }
}

class ScriptTransformerFn(ext: String, name: String, engine: Invocable) extends TransformerFn {

  override def names: Seq[String] = Seq(s"$ext:$name")

  override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
    engine.asInstanceOf[Invocable].invokeFunction(name, args.asInstanceOf[Array[_ <: Object]]: _*)
  }
}
