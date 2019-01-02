/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert

import java.io.{Closeable, IOException, InputStream}
import java.nio.charset.StandardCharsets

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.geotools.factory.Hints
import org.locationtech.geomesa.convert.Modes.ErrorMode
import org.locationtech.geomesa.convert.Modes.ParseMode
import org.locationtech.geomesa.convert.Transformers._
import org.locationtech.geomesa.convert.ValidationMode.ValidationMode
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.transforms.Expression.LiteralNull
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@deprecated("Replaced with org.locationtech.geomesa.convert2.Field")
trait Field extends org.locationtech.geomesa.convert2.Field {
  def name: String
  def transform: Transformers.Expr
  override def transforms: Option[Expression] = Option(transform)
  override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = transform.eval(args)
}

/**
  * Wrapper to present convert v2 fields as convert v1
  *
  * @param field v2 field
  */
case class FieldWrapper(field: org.locationtech.geomesa.convert2.Field) extends Field {
  override def name: String = field.name
  override def transform: Expr = field.transforms.map(ExpressionWrapper.apply).orNull
  override def transforms: Option[Expression] = field.transforms
  override def eval(args: Array[Any])(implicit ec: EvaluationContext): Any = field.eval(args)(ec)
}

@deprecated("Replaced with org.locationtech.geomesa.convert2.BasicField")
case class SimpleField(name: String, transform: Transformers.Expr) extends Field {
  override val transforms: Option[Expression] = Option(transform)
  override def toString: String = s"$name = $transform"
}

object StandardOption extends Enumeration {
  type StandardOption = Value

  @deprecated
  val Validating = Value("validating")

  @deprecated
  val ValidationModeOpt = Value("validation-mode")

  val ValidatorsOpt = Value("validators")
  val ErrorModeOpt  = Value("error-mode")
  val LineModeOpt   = Value("line-mode")
  val ParseModeOpt  = Value("parse-mode")
  val VerboseOpt    = Value("verbose")

  implicit class StandardOptionValue(opt: Value) {
    def path = s"options.$opt"
  }
}

@deprecated("Replaced with org.locationtech.geomesa.convert.Modes.ErrorMode")
object ValidationMode extends Enumeration {
  type ValidationMode = Value
  val SkipBadRecords = Value(ErrorMode.SkipBadRecords.toString)
  val RaiseErrors    = Value(ErrorMode.RaiseErrors.toString)
  val Default        = SkipBadRecords
}

case class ConvertParseOpts(parseMode: ParseMode,
                            validator: SimpleFeatureValidator,
                            validationMode: ValidationMode,
                            verbose: Boolean)

@deprecated("Replaced with org.locationtech.geomesa.convert2.SimpleFeatureConverterFactory")
trait SimpleFeatureConverterFactory[I] {
  def canProcess(conf: Config): Boolean
  def buildConverter(sft: SimpleFeatureType, conf: Config): SimpleFeatureConverter[I]
}

@deprecated("Replaced with org.locationtech.geomesa.convert2.AbstractConverterFactory")
abstract class AbstractSimpleFeatureConverterFactory[I] extends SimpleFeatureConverterFactory[I] with LazyLogging {

  override def canProcess(conf: Config): Boolean =
    if (conf.hasPath("type")) conf.getString("type").equals(typeToProcess) else false

  override def buildConverter(sft: SimpleFeatureType, conf: Config): SimpleFeatureConverter[I] = {
    val idBuilder = buildIdBuilder(conf)
    val fields = buildFields(conf)

    val userDataBuilder = buildUserDataBuilder(conf)
    val cacheServices = buildCacheService(conf)
    val parseOpts = getParsingOptions(conf, sft)
    buildConverter(sft, conf, idBuilder, fields, userDataBuilder, cacheServices, parseOpts)
  }

  protected def typeToProcess: String

  protected def buildConverter(sft: SimpleFeatureType,
                               conf: Config,
                               idBuilder: Expr,
                               fields: IndexedSeq[Field],
                               userDataBuilder: Map[String, Expr],
                               cacheServices: Map[String, EnrichmentCache],
                               parseOpts: ConvertParseOpts): SimpleFeatureConverter[I]

  protected def buildFields(conf: Config): IndexedSeq[Field] =
    conf.getConfigList("fields").map(buildField).toIndexedSeq

  protected def buildField(field: Config): Field =
    SimpleField(field.getString("name"), Transformers.parseTransform(field.getString("transform")))

  protected def buildIdBuilder(conf: Config): Expr = {
    if (conf.hasPath("id-field")) {
      Transformers.parseTransform(conf.getString("id-field"))
    } else {
      Transformers.parseTransform("null")
    }
  }

  protected def buildUserDataBuilder(conf: Config): Map[String, Expr] = {
    if (conf.hasPath("user-data")) {
      conf.getConfig("user-data").entrySet.map { e =>
        e.getKey -> Transformers.parseTransform(e.getValue.unwrapped().toString)
      }.toMap
    } else {
      Map.empty
    }
  }

  protected def buildCacheService(config: Config): Map[String, EnrichmentCache] = {
    if(config.hasPath("caches")) {
      val cacheConfig = config.getConfig("caches")
      cacheConfig.root().keys.map { k =>
        val specificConf = cacheConfig.getConfig(k)
        k -> EnrichmentCache(specificConf)
      }.toMap
    } else {
      Map.empty
    }
  }

  protected def getParsingOptions(conf: Config, sft: SimpleFeatureType): ConvertParseOpts = {
    val verbose = if (conf.hasPath(StandardOption.VerboseOpt.path)) conf.getBoolean(StandardOption.VerboseOpt.path) else false
    val opts = ConvertParseOpts(getParseMode(conf), getValidator(conf, sft), getValidationMode(conf), verbose = verbose)
    lazy val SimpleFeatureValidator(validators@_*) = opts.validator // unapplySeq to extract names
    logger.info(s"Using ParseMode ${opts.parseMode} with error mode ${opts.validationMode} and validator ${validators.mkString(", ")}")
    opts
  }

  // noinspection ScalaDeprecation
  protected def getValidator(conf: Config, sft: SimpleFeatureType): SimpleFeatureValidator = {
    val validators: Seq[String] =
      if (conf.hasPath(StandardOption.Validating.path) && conf.hasPath(StandardOption.ValidatorsOpt.path)) {
        // This is when you have the old deprecated key...
        throw new IllegalArgumentException(s"Converter should not have both ${StandardOption.Validating.path}(deprecated) and " +
          s"${StandardOption.ValidatorsOpt.path} config keys")
      } else if (conf.hasPath(StandardOption.ValidatorsOpt.path)) {
        conf.getStringList(StandardOption.ValidatorsOpt.path)
      } else if (conf.hasPath(StandardOption.Validating.path)) {
        logger.warn(s"Using deprecated validation key ${StandardOption.Validating.path}")
        if (conf.getBoolean(StandardOption.Validating.path)) {
          Seq("has-geo", "has-dtg")
        } else {
          Seq("none")
        }
      } else {
        Seq("has-geo", "has-dtg")
      }
    val validator = SimpleFeatureValidator(validators)
    validator.init(sft)
    validator
  }

  protected def getParseMode(conf: Config): ParseMode =
    if (conf.hasPath(StandardOption.ParseModeOpt.path)) {
      val modeStr = conf.getString(StandardOption.ParseModeOpt.path)
      try {
        ParseMode.withName(modeStr)
      } catch {
        case _: NoSuchElementException => throw new IllegalArgumentException(s"Unknown parse mode $modeStr")
      }
    } else {
      ParseMode.Default
    }

  protected def getValidationMode(conf: Config): ValidationMode = {
    if (conf.hasPath(StandardOption.ErrorModeOpt.path)) {
      val modeStr = conf.getString(StandardOption.ErrorModeOpt.path)
      try { ValidationMode.withName(modeStr) } catch {
        case _: NoSuchElementException => throw new IllegalArgumentException(s"Unknown error mode $modeStr")
      }
    } else if (conf.hasPath(StandardOption.ValidationModeOpt.path)) {
      logger.warn(s"Using deprecated option ${StandardOption.ValidationModeOpt}. Prefer ${StandardOption.ErrorModeOpt}")
      val modeStr = conf.getString(StandardOption.ValidationModeOpt.path)
      try { ValidationMode.withName(modeStr) } catch {
        case _: NoSuchElementException => throw new IllegalArgumentException(s"Unknown validation mode $modeStr")
      }
    } else {
      ValidationMode.Default
    }
  }

}

@deprecated("Replaced with org.locationtech.geomesa.convert2.SimpleFeatureConverter")
trait SimpleFeatureConverter[I] extends Closeable {

  /**
   * Result feature type
   */
  def targetSFT: SimpleFeatureType

  /**
    * Enrichment caches
    *
    * @return
    */
  def caches: Map[String, EnrichmentCache]

  /**
   * Stream process inputs into simple features
   */
  def processInput(is: Iterator[I], ec: EvaluationContext = createEvaluationContext()): Iterator[SimpleFeature]

  def processSingleInput(i: I, ec: EvaluationContext = createEvaluationContext()): Iterator[SimpleFeature]

  def process(is: InputStream, ec: EvaluationContext = createEvaluationContext()): Iterator[SimpleFeature]

  /**
   * Creates a context used for processing
   */
  def createEvaluationContext(globalParams: Map[String, Any] = Map.empty,
                              counter: Counter = new DefaultCounter): EvaluationContext = {
    val keys = globalParams.keys.toIndexedSeq
    val values = keys.map(globalParams.apply).toArray
    EvaluationContext(keys, values, counter, caches)
  }

  override def close(): Unit = {}
}

@deprecated("Replaced with org.locationtech.geomesa.convert2.SimpleFeatureConverter")
object SimpleFeatureConverter {

  type Dag = scala.collection.mutable.Map[Field, Set[Field]]

  /**
    * Add the dependencies of a field to a graph
    *
    * @param field field to add
    * @param fieldMap field lookup map
    * @param dag graph
    */
  def addDependencies(field: Field, fieldMap: Map[String, Field], dag: Dag): Unit = {
    if (!dag.contains(field)) {
      val deps = Option(field.transform).toSeq.flatMap(_.dependenciesOf(Set(field), fieldMap)).toSet
      dag.put(field, deps)
      deps.foreach(addDependencies(_, fieldMap, dag))
    }
  }

  /**
    * Returns vertices in topological order.
    *
    * Note: will cause an infinite loop if there are circular dependencies
    *
    * @param dag graph
    * @return ordered vertices
    */
  def topologicalOrder(dag: Dag): IndexedSeq[Field] = {
    val res = ArrayBuffer.empty[Field]
    val remaining = dag.keys.to[scala.collection.mutable.Queue]
    while (remaining.nonEmpty) {
      val next = remaining.dequeue()
      if (dag(next).forall(res.contains)) {
        res.append(next)
      } else {
        remaining.enqueue(next) // put at the back of the queue
      }
    }
    res.toIndexedSeq
  }
}

/**
 * Base trait to create a simple feature converter
 */
@deprecated("Replaced with org.locationtech.geomesa.convert2.AbstractConverter")
trait ToSimpleFeatureConverter[I] extends SimpleFeatureConverter[I] with LazyLogging {

  def targetSFT: SimpleFeatureType
  def inputFields: Seq[Field]
  def idBuilder: Expr
  def userDataBuilder: Map[String, Expr]
  def caches: Map[String, EnrichmentCache]
  def fromInputType(i: I, ec: EvaluationContext): Iterator[Array[Any]]
  def parseOpts: ConvertParseOpts

  protected val validate: (SimpleFeature, EvaluationContext) => SimpleFeature =
    (sf: SimpleFeature, ec: EvaluationContext) => {
      val error = parseOpts.validator.validate(sf)
      if (error == null) { sf } else {
        val msg = s"Invalid SimpleFeature on line ${ec.counter.getLineCount}: $error"
        if (parseOpts.validationMode == ValidationMode.RaiseErrors) {
          throw new IOException(msg)
        } else {
          logger.debug(msg)
          null
        }
      }
    }

  protected val requiredFields: IndexedSeq[Field] = {
    import SimpleFeatureConverter.{addDependencies, topologicalOrder}

    val fieldNameMap = inputFields.map(f => (f.name, f)).toMap
    val dag = scala.collection.mutable.Map.empty[Field, Set[Field]]

    // compute only the input fields that we need to deal with to populate the simple feature
    targetSFT.getAttributeDescriptors.foreach { ad =>
      fieldNameMap.get(ad.getLocalName).foreach(addDependencies(_, fieldNameMap, dag))
    }

    // add id field and user data deps - these will be evaluated last so we only need to add their deps
    val others = (userDataBuilder.values.toSeq :+ idBuilder).flatMap(_.dependenciesOf(Set.empty, fieldNameMap))
    others.foreach(addDependencies(_, fieldNameMap, dag))

    // use a topological ordering to ensure that dependencies are evaluated before the fields that require them
    topologicalOrder(dag)
  }

  protected val requiredFieldsCount: Int = requiredFields.length
  protected val requiredFieldsIndices: IndexedSeq[Int] = requiredFields.map(f => targetSFT.indexOf(f.name))

  /**
   * Convert input values into a simple feature with attributes
   */
  def convert(t: Array[Any], ec: EvaluationContext): SimpleFeature = {
    val sfValues = Array.ofDim[AnyRef](targetSFT.getAttributeCount)

    var i = 0
    while (i < requiredFieldsCount) {
      try {
        ec.set(i, requiredFields(i).eval(t)(ec))
      } catch {
        case e: Exception =>
          val msg = if (parseOpts.verbose) {
            val valuesStr = if (t.length > 0) t.tail.mkString(", ") else ""
            s"Failed to evaluate field '${requiredFields(i).name}' " +
              s"on line ${ec.counter.getLineCount} using values:\n" +
              s"${t.headOption.orNull}\n[$valuesStr]"  // head is the whole record
          } else {
            s"Failed to evaluate field '${requiredFields(i).name}' on line ${ec.counter.getLineCount}"
          }
          if (parseOpts.validationMode == ValidationMode.SkipBadRecords) {
            if (parseOpts.verbose) { logger.debug(msg, e) } else { logger.debug(msg) }
            return null
          } else {
            throw new IOException(msg, e)
          }
      }
      val sftIndex = requiredFieldsIndices(i)
      if (sftIndex != -1) {
        sfValues.update(sftIndex, ec.get(i).asInstanceOf[AnyRef])
      }
      i += 1
    }

    val sf = buildFeature(t, sfValues, ec)

    validate(sf, ec)
  }

  private def buildFeature(t: Array[Any], sfValues: Array[AnyRef], ec: EvaluationContext): SimpleFeature = {
    val sf = idBuilder match {
      case ExpressionWrapper(LiteralNull) => new ScalaSimpleFeature(targetSFT, "", sfValues) // empty feature id will be replaced with an auto-gen one
      case _       =>
        val id = idBuilder.eval(t)(ec).asInstanceOf[String]
        val res = new ScalaSimpleFeature(targetSFT, id, sfValues)
        res.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        res
    }
    userDataBuilder.foreach { case (k, v) => sf.getUserData.put(k, v.eval(t)(ec).asInstanceOf[AnyRef]) }
    sf
  }

  /**
   * Process a single input (e.g. line)
   */
  override def processSingleInput(i: I, ec: EvaluationContext): Iterator[SimpleFeature] = {
    ec.clear()
    ec.counter.incLineCount()

    val attributes = try { fromInputType(i, ec) } catch {
      case e: Exception =>
        logger.warn(s"Failed to parse input '$i'", e)
        ec.counter.incFailure()
        Iterator.empty
    }

    attributes.flatMap { a =>
      convert(a, ec) match {
        case null => ec.counter.incFailure(); Iterator.empty
        case sf   => ec.counter.incSuccess(); Iterator.single(sf)
      }
    }
  }

  override def createEvaluationContext(globalParams: Map[String, Any], counter: Counter): EvaluationContext = {
    val globalKeys = globalParams.keys.toSeq
    val names = requiredFields.map(_.name) ++ globalKeys
    val values = Array.ofDim[Any](names.length)
    // note, globalKeys are maintained even through EvaluationContext.clear()
    globalKeys.zipWithIndex.foreach { case (k, i) => values(requiredFields.length + i) = globalParams(k) }
    new EvaluationContextImpl(names, values, counter, caches)
  }

  override def processInput(is: Iterator[I], ec: EvaluationContext): Iterator[SimpleFeature] = {
    parseOpts.parseMode match {
       case ParseMode.Incremental  =>
         is.flatMap(i => processSingleInput(i, ec))
       case ParseMode.Batch =>
         val ret = mutable.ListBuffer.empty[SimpleFeature]
         is.foreach(i => ret ++= processSingleInput(i, ec))
         ret.iterator
    }
  }

  override def close(): Unit = caches.values.foreach(CloseWithLogging.apply)
}

@deprecated("Replaced with org.locationtech.geomesa.convert2.AbstractConverter")
trait LinesToSimpleFeatureConverter extends ToSimpleFeatureConverter[String] {

  override def process(is: InputStream, ec: EvaluationContext): Iterator[SimpleFeature] =
    processInput(IOUtils.lineIterator(is, StandardCharsets.UTF_8.displayName), ec)

}
