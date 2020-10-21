package org.kairosdb.rollup;

import com.google.inject.Inject;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class RollUpAssignmentStoreImpl implements RollUpAssignmentStore {
    public static final Logger logger = LoggerFactory.getLogger(RollUpAssignmentStoreImpl.class);

    static final String SERVICE = "_Rollups";
    static final String SERVICE_KEY_ASSIGNMENTS = "Assignment";

    private final ServiceKeyStore serviceKeyStore;

    @Inject
    public RollUpAssignmentStoreImpl(final ServiceKeyStore serviceKeyStore) {
        this.serviceKeyStore = checkNotNull(serviceKeyStore, "serviceKeyStore cannot be null");
    }

    @Override
    public long getLastModifiedTime()
            throws RollUpException {
        try {
            final Date lastModifiedTime = serviceKeyStore.getServiceKeyLastModifiedTime(SERVICE, SERVICE_KEY_ASSIGNMENTS);
            if (lastModifiedTime != null) {
                return lastModifiedTime.getTime();
            }
            return 0L;
        } catch (final DatastoreException e) {
            throw new RollUpException("Could not read from service keystore", e);
        }
    }

    @Override
    public Set<String> getAssignmentIds()
            throws RollUpException {
        try {
            final Set<String> assignedIds = new HashSet<>();
            final Iterable<String> keys = serviceKeyStore.listKeys(SERVICE, SERVICE_KEY_ASSIGNMENTS);
            for (final String key : keys) {
                assignedIds.add(key);
            }
            return assignedIds;
        } catch (final DatastoreException e) {
            throw new RollUpException("Could not read from service keystore", e);
        }
    }

    @Override
    public Map<String, String> getAssignments()
            throws RollUpException {
        try {
            final Map<String, String> assignments = new HashMap<>();
            final Iterable<String> keys = serviceKeyStore.listKeys(SERVICE, SERVICE_KEY_ASSIGNMENTS);
            for (final String key : keys) {
                assignments.put(key, serviceKeyStore.getValue(SERVICE, SERVICE_KEY_ASSIGNMENTS, key).getValue());
            }
            return assignments;
        } catch (final DatastoreException e) {
            throw new RollUpException("Could not read from service keystore", e);
        }
    }

    @Override
    public Set<String> getAssignedIds(final String host)
            throws RollUpException {
        final Set<String> assignedTasks = new HashSet<>();
        try {
            final Iterable<String> keys = serviceKeyStore.listKeys(SERVICE, SERVICE_KEY_ASSIGNMENTS);
            for (final String key : keys) {
                final String assigned = serviceKeyStore.getValue(SERVICE, SERVICE_KEY_ASSIGNMENTS, key).getValue();
                if (assigned.equals(host)) {
                    assignedTasks.add(key);
                }
            }
            return assignedTasks;
        } catch (final DatastoreException e) {
            throw new RollUpException("Could not read from service keystore", e);
        }
    }

    @Override
    public void setAssignment(final String unassignedId, final String hostName)
            throws RollUpException {
        checkNotNullOrEmpty(unassignedId, "unassignedId cannot be null or empty");
        checkNotNullOrEmpty(hostName, "hostName cannot be null or empty");

        try {
            serviceKeyStore.setValue(SERVICE, SERVICE_KEY_ASSIGNMENTS, unassignedId, hostName);
        } catch (final DatastoreException e) {
            throw new RollUpException("Could not write assignment to service keystore. Id: " + unassignedId + " value: " + hostName, e);
        }
    }

    @Override
    public void removeAssignments(final Set<String> ids)
            throws RollUpException {
        try {
            for (final String id : ids) {
                serviceKeyStore.deleteKey(SERVICE, SERVICE_KEY_ASSIGNMENTS, id);
            }
        } catch (final DatastoreException e) {
            throw new RollUpException("Could not delete ids.", e);
        }
    }
}
