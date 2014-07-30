/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package geomesa.tools

class Tools {

}

object Tools extends App {
  val parser = new scopt.OptionParser[Config]("geomesa-tools") {
    head("GeoMesa Tools", "1.0")
    help("help").text("show help command")
    cmd("export") action { (_, c) =>
      c.copy(mode = "export") } text("export is a command") children(
      )
    cmd("features") action { (_, c) =>
      c.copy(mode = "features") } text("features is a command") children(
      )
    cmd("injest") action { (_, c) =>
      c.copy(mode = "injest") } text("Injest is a command") children(
      )
  }
}

case class Config(mode: String = null)



