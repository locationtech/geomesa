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

package geomesa.plugin.ui.components

import org.apache.wicket.ajax.AjaxRequestTarget
import org.apache.wicket.ajax.markup.html.form.AjaxSubmitLink
import org.apache.wicket.markup.html.form.Form
import org.apache.wicket.markup.html.form.upload.FileUploadField
import org.apache.wicket.markup.html.panel.Panel
import org.apache.wicket.util.lang.Bytes

abstract class FileUploadComponent(id: String) extends Panel(id) {

  def onSubmit(target: AjaxRequestTarget, fileField: FileUploadField):Unit = ???

  private val fileForm = new Form("fileForm") {

    setMaxSize(Bytes.megabytes(1));

    val fileField = new FileUploadField("file")

    add(fileField)

    add(new AjaxSubmitLink("load", this) {
      protected override def onError(target: AjaxRequestTarget, form: Form[_]) {
        super.onError(target, form)
        target.addComponent(form)
      }

      protected def onSubmit(target: AjaxRequestTarget, form: Form[_]) {
        FileUploadComponent.this.onSubmit(target, fileField)
      }
    })
  }

  add(fileForm)

}
