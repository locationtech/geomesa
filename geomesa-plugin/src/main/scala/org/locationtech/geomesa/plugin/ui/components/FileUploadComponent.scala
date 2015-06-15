/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.ui.components

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
