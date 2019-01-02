/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.scripting

import java.io.File
import java.net.URI
import java.nio.file.FileSystems
import java.util.Collections
import javax.script.{Invocable, ScriptContext, ScriptEngine, ScriptEngineManager}

import com.google.common.io.Files
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.commons.io.{FileUtils, IOUtils}
import org.locationtech.geomesa.convert.{EvaluationContext, TransformerFn, TransformerFunctionFactory}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import scala.collection.JavaConversions._

/**
  * Provides TransformerFunctions that execute javax.scripts compatible functions defined
  * on the classpath or in external directories.  Scripting languages are determined by
  * the extension of the file.  For instance, 'name.js' will be interpreted as a javascript file.
  */
class ScriptingFunctionFactory extends TransformerFunctionFactory with LazyLogging {

  import ScriptingFunctionFactory._

  lazy val functions = init()

  private def init() = {
    val curClassLoader = Thread.currentThread().getContextClassLoader
    val scriptURIs = loadScriptsFromEnvironment ++ loadScripts(curClassLoader)

    logger.debug(s"Script URIs found: ${scriptURIs.map(_.toString).mkString(",")}")
    val mgr = new ScriptEngineManager()
    scriptURIs
      .map { f =>
        // Always use the scheme specific part for URIs
        val ext = Files.getFileExtension(f.getSchemeSpecificPart)
        (f, ext)
      }.groupBy { case (_, ext) => ext }
      .flatMap { case (ext, files) =>
        val engine = Option(mgr.getEngineByExtension(ext))
        engine.map { e => evaluateScriptsForEngine(curClassLoader, files, e, ext) }.getOrElse(Seq())
      }.toList
  }

  private def evaluateScriptsForEngine(loader: ClassLoader, files: Seq[(URI, String)], e: ScriptEngine, ext: String) = {
    files.foreach { case (f, _) => evalScriptFile(loader, e, f) }
    e.getBindings(ScriptContext.ENGINE_SCOPE).map { case (k, v) =>
      new ScriptTransformerFn(ext, k, e.asInstanceOf[Invocable])
    }
  }

  // TODO try with no catch and logging...evaluate whats happening here
  def evalScriptFile(loader: ClassLoader, e: ScriptEngine, f: URI): AnyRef =
    try {
      val lines = IOUtils.toString(f, "UTF-8")
      e.eval(lines)
    }
}

object ScriptingFunctionFactory extends LazyLogging {
  final val ConvertScriptsPathProperty = "geomesa.convert.scripts.path"
  final val ConvertScriptsClassPath    = "geomesa-convert-scripts"

  /**
    * <p>Load scripts from the environment using the property "geomesa.convert.scripts.path"
    * Entries are colon (:) separated. Entries can be files or directories. Directories will
    * be recursively searched for script files. The extension of script files defines what
    * kind of script they are (e.g. js = javascript)</p>
    */
  def loadScriptsFromEnvironment: Seq[URI] = {
    val v = SystemProperty(ConvertScriptsPathProperty).get
    if (v == null) Seq.empty[URI]
    else {
      v.split(":")
        .map { d => new File(d) }
        .filter { f => f.exists() && f.canRead && (if (f.isDirectory) f.canExecute else true) }
        .flatMap { f =>
          if (f.isDirectory) {
            FileUtils.listFiles(f, TrueFileFilter.TRUE, TrueFileFilter.TRUE).map(_.toURI)
          } else {
            Seq(f.toURI)
          }
        }
    }
  }

  /**
    * Load scripts from the resource geomesa-convert-scripts from a classloader. To use this
    * create a folder in the jar named "geomesa-convert-scripts" and place script files
    * within that directory.
    *
    * @param loader
    * @return
    */
  def loadScripts(loader: ClassLoader): Seq[URI] = {
    Option(loader.getResources(ConvertScriptsClassPath + "/")).toSeq.flatten.flatMap { url =>
      val uri = url.toURI
      uri.getScheme match {
        case "jar" =>
          val fs = FileSystems.newFileSystem(uri, Collections.emptyMap[String, AnyRef](), loader)
          val p = fs.getPath(ConvertScriptsClassPath + "/")
          val uris = java.nio.file.Files.walk(p).iterator().toSeq
          val files = uris.filterNot(java.nio.file.Files.isDirectory(_)).map(_.toUri)
          logger.debug(s"Loaded scripts ${files.mkString(",")} from jar ${uri.toString}")
          files
        case "file" =>
          IOUtils.readLines(url.openStream(), "UTF-8")
            .map { s => loader.getResource(s"$ConvertScriptsClassPath/$s").toURI }
      }
    }
  }
}

class ScriptTransformerFn(ext: String, name: String, engine: Invocable) extends TransformerFn {

  override def names: Seq[String] = Seq(s"$ext:$name")

  override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
    engine.asInstanceOf[Invocable].invokeFunction(name, args.asInstanceOf[Array[_ <: Object]]: _*)
  }
}
