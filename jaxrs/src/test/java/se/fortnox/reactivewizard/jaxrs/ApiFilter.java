package se.fortnox.reactivewizard.jaxrs;

import se.fortnox.reactivewizard.CollectionOptions;

import javax.ws.rs.QueryParam;

public class ApiFilter extends CollectionOptions {

    @QueryParam("property")
    private String property;

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }
}
