package geomesa.plugin

import org.apache.wicket.behavior.SimpleAttributeModifier
import org.apache.wicket.markup.html.form.{FormComponent, Form}
import org.apache.wicket.markup.html.panel.Panel
import org.apache.wicket.model.{ResourceModel, IModel}
import org.geoserver.web.data.store.StoreEditPanel
import org.geoserver.web.data.store.panel.{ParamPanel, PasswordParamPanel, TextParamPanel}
import org.geoserver.web.util.MapModel
import org.geotools.data.DataAccessFactory.Param

/**
 * Created with IntelliJ IDEA.
 * User: fhp
 * Date: 7/29/14
 * Time: 3:30 PM
 * To change this template use File | Settings | File Templates.
 */
abstract class GeoMesaStoreEditPanel (componentId: String, storeEditForm: Form[_])
    extends StoreEditPanel(componentId, storeEditForm) {

  def addTextPanel(paramsModel: IModel[_], param: Param): FormComponent[_] = {
    val paramName = param.key
    val resourceKey = getClass.getSimpleName + "." + paramName
    val required = param.required
    val textParamPanel =
      new TextParamPanel(paramName,
        new MapModel(paramsModel, paramName).asInstanceOf[IModel[_]],
        new ResourceModel(resourceKey, paramName), required)
    addPanel(textParamPanel, param, resourceKey)
  }

  def addPasswordPanel(paramsModel: IModel[_], param: Param): FormComponent[_] = {
    val paramName = param.key
    val resourceKey = getClass.getSimpleName + "." + paramName
    val required = param.required
    val passParamPanel =
      new PasswordParamPanel(paramName,
        new MapModel(paramsModel, paramName).asInstanceOf[IModel[_]],
        new ResourceModel(resourceKey, paramName), required)
    addPanel(passParamPanel, param, resourceKey)
  }

  def addPanel(paramPanel: Panel with ParamPanel, param: Param, resourceKey: String): FormComponent[_] = {
    paramPanel.getFormComponent.setType(classOf[String])
    val defaultTitle = String.valueOf(param.description)
    val titleModel = new ResourceModel(resourceKey + ".title", defaultTitle)
    val title = String.valueOf(titleModel.getObject)
    paramPanel.add(new SimpleAttributeModifier("title", title))
    add(paramPanel)
    paramPanel.getFormComponent
  }
}
