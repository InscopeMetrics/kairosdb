package org.kairosdb.core.datastore;

import org.kairosdb.core.http.rest.BeanValidationException;
import org.kairosdb.core.http.rest.json.QueryParser;

/**
 * Created with IntelliJ IDEA.
 * User: bhawkins
 * Date: 10/20/13
 * Time: 6:51 AM
 * To change this template use File | Settings | File Templates.
 */
public enum Order {
    ASC("asc"),
    DESC("desc");

    private final String m_text;

    Order(final String text) {
        m_text = text;
    }

    public static Order fromString(final String text, final String context) throws BeanValidationException {
        Order ret = null;

        if (text != null) {
            for (final Order o : Order.values()) {
                if (text.equalsIgnoreCase(o.m_text)) {
                    ret = o;
                    break;
                }
            }
        }


        if (ret == null) {
            throw new BeanValidationException(new QueryParser.SimpleConstraintViolation("order", "must be either 'asc' or 'desc'"), context);
        } else
            return (ret);
    }

    public String getText() {
        return (m_text);
    }
}
