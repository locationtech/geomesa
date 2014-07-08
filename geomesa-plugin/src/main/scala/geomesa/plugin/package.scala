/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package geomesa

package object plugin {

  object properties {
    val FS_DEFAULTFS                     = "fs.defaultFS"
    val YARN_RESOURCEMANAGER_ADDRESS     = "yarn.resourcemanager.address"
    val YARN_SCHEDULER_ADDRESS           = "yarn.resourcemanager.scheduler.address"
    val MAPREDUCE_FRAMEWORK_NAME         = "mapreduce.framework.name"

    def values: List[String] = {
      List(FS_DEFAULTFS,
           YARN_RESOURCEMANAGER_ADDRESS,
           YARN_SCHEDULER_ADDRESS,
           MAPREDUCE_FRAMEWORK_NAME)
    }
  }

}
