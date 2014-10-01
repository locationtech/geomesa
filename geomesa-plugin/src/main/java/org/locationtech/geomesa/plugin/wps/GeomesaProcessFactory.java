package org.locationtech.geomesa.plugin.wps;

import org.geoserver.wps.jts.SpringBeanProcessFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeomesaProcessFactory extends SpringBeanProcessFactory {
    private Logger log = LoggerFactory.getLogger(GeomesaProcessFactory.class);

    public GeomesaProcessFactory(String title, String namespace, Class markerInterface) {
        super(title, namespace, markerInterface);
        log.info("Created GeomesaProcessFactory");
    }
}
