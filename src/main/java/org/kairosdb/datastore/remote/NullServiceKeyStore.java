package org.kairosdb.datastore.remote;

import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.exception.DatastoreException;

import java.util.Date;

/**
 * Created by bhawkins on 4/29/17.
 */
public class NullServiceKeyStore implements ServiceKeyStore {
    @Override
    public void setValue(final String service, final String serviceKey, final String key, final String value) throws DatastoreException {
        throw new DatastoreException("Method not implemented.");
    }

    @Override
    public ServiceKeyValue getValue(final String service, final String serviceKey, final String key) throws DatastoreException {
        throw new DatastoreException("Method not implemented.");
    }

    @Override
    public Iterable<String> listServiceKeys(final String service) throws DatastoreException {
        throw new DatastoreException("Method not implemented.");
    }

    @Override
    public Iterable<String> listKeys(final String service, final String serviceKey) throws DatastoreException {
        throw new DatastoreException("Method not implemented.");
    }

    @Override
    public Iterable<String> listKeys(final String service, final String serviceKey, final String keyStartsWith) throws DatastoreException {
        throw new DatastoreException("Method not implemented.");
    }

    @Override
    public void deleteKey(final String service, final String serviceKey, final String key) throws DatastoreException {
        throw new DatastoreException("Method not implemented.");
    }

    @Override
    public Date getServiceKeyLastModifiedTime(final String service, final String serviceKey) throws DatastoreException {
        throw new DatastoreException("Method not implemented.");
    }
}
