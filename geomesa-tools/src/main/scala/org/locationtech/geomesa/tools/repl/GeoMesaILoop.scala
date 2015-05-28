/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.tools.repl

import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.jobs.scalding._

import scala.tools.nsc.interpreter.ILoop

class GeoMesaILoop extends ILoop {

  override def prompt = Console.GREEN + "\ngeomesa> " + Console.RESET

  override def printWelcome(): Unit = echo(GeoMesaILoop.welcome)

  override def createInterpreter() {
    super.createInterpreter()
    addThunk {
      intp.beQuietDuring {
        intp.interpret(GeoMesaILoop.imports)
      }
      intp.beSilentDuring {
        intp.interpret("classOf[scala.tools.jline.console.completer.Completer]") match {
          case scala.tools.nsc.interpreter.Results.Error =>
            println("\nWARNING: Could not load jline. For better console usability, " +
                "run the install-jline script in $GEOMESA_HOME/bin.")
          case _ => // ok
        }
      }
    }
  }
}

object GeoMesaILoop {
  val welcome =
    """
     |o                              7777  777777777
      |                      77777777   7777 77777777777777
      |                      777  77     77  77    7    77777777777
      |                     777   7      77             77I77     77
      |                   77777   7      77              77 7      77
      |    7777777777777777 7I           7                7  7  777 77
      |   7I          I77               77                 7    77 7 7  77777777
      | 7            77                 7                        77I  77I       777
      |             r   __ _  ___  ___ b _ __ ___   ___  ___  __ _x
      |             r  / _` |/ _ \/ _ \b| '_ ` _ \ / _ \/ __|/ _` |x
      |             r | (_| |  __/ (_) b| | | | | |  __/\__ \ (_| |x
      |             r  \__, |\___|\___/b|_| |_| |_|\___||___/\__,_|x
      |             r  |___/x"""
        .stripMargin
        .replaceAll("o", Console.YELLOW)
        .replaceAll("r", Console.RED + Console.BOLD)
        .replaceAll("b", Console.RESET + Console.RED)
        .replaceAll("x", Console.RESET)

  // default imports that will be in scope for each session
  val imports =
    Seq("com.twitter.scalding._",
        "com.twitter.scalding.ReplImplicits._",
        "com.twitter.scalding.ReplImplicitContext._",
        classOf[GeoMesaSource].getName,
        classOf[GeoMesaInputOptions].getName,
        classOf[GeoMesaOutputOptions].getName,
        classOf[AccumuloSource].getName,
        classOf[AccumuloInputOptions].getName,
        classOf[AccumuloOutputOptions].getName,
        classOf[SerializedRange].getName,
        classOf[SerializedColumn].getName,
        classOf[Endpoint].getName,
        classOf[AccumuloDataStore].getName,
        "org.locationtech.geomesa.utils.geotools.Conversions._",
        "org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes",
        "org.geotools.data._",
        "org.geotools.filter.text.ecql.ECQL",
        "scala.collection.JavaConversions._").mkString("import ", ", ", "")
}