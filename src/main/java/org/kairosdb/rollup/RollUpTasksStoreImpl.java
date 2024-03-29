package org.kairosdb.rollup;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.http.rest.BeanValidationException;
import org.kairosdb.core.http.rest.QueryException;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class RollUpTasksStoreImpl implements RollUpTasksStore {
    public static final Logger logger = LoggerFactory.getLogger(RollUpTasksStoreImpl.class);

    static final String OLD_FILENAME = "/tmp/rollup.config";
    static final String SERVICE = "_Rollups";
    static final String SERVICE_KEY_CONFIG = "Config";
    private final QueryParser parser;
    private final ServiceKeyStore keyStore;

    @Inject
    public RollUpTasksStoreImpl(final ServiceKeyStore keyStore, final QueryParser parser)
            throws RollUpException {
        this.keyStore = checkNotNull(keyStore, "keyStore cannot be null");
        this.parser = checkNotNull(parser, "parser cannot be null");

        try {
            importFromOldFile();
        } catch (final RollUpException | IOException | QueryException e) {
            throw new RollUpException("Failed to complete import from old roll-up format to new format in the datastore.", e);
        }
    }

    @Override
    public void write(final List<RollupTask> tasks)
            throws RollUpException {
        checkNotNull(tasks, "tasks cannot be null");
        try {
            for (final RollupTask task : tasks) {
                checkNotNull(task, "task cannot be null");
                keyStore.setValue(SERVICE, SERVICE_KEY_CONFIG, task.getId(), task.getJson());
            }
        } catch (final DatastoreException e) {
            throw new RollUpException("Failed to write roll-up tasks to the service keystore", e);
        }
    }

    @Override
    public Map<String, RollupTask> read()
            throws RollUpException {
        try {
            final Map<String, RollupTask> tasks = new HashMap<>();
            final Iterable<String> keys = keyStore.listKeys(SERVICE, SERVICE_KEY_CONFIG);
            for (final String key : keys) {
                final ServiceKeyValue serviceKeyValue = keyStore.getValue(SERVICE, SERVICE_KEY_CONFIG, key);
                final String value = serviceKeyValue.getValue();
                if (!Strings.isNullOrEmpty(value)) {

                    final RollupTask task = parser.parseRollupTask(value);
                    task.setLastModified(serviceKeyValue.getLastModified().getTime());
                    tasks.put(key, task);
                } else {
                    logger.error("Null or empty rollup key");
                }
            }

            return tasks;
        } catch (final DatastoreException e) {
            throw new RollUpException("Failed to read roll-up tasks from the service keystore", e);
        } catch (final BeanValidationException | QueryException e) {
            throw new RollUpException("Failed to read rollups from the keystore", e);
        }
    }

    @Override
    public void remove(final String id)
            throws RollUpException {
        try {
            keyStore.deleteKey(SERVICE, SERVICE_KEY_CONFIG, id);
        } catch (final DatastoreException e) {
            throw new RollUpException("Failed to remove roll-up tasks from the service keystore", e);
        }
    }

    @Override
    public RollupTask read(final String id)
            throws RollUpException {
        try {
            final ServiceKeyValue value = keyStore.getValue(SERVICE, SERVICE_KEY_CONFIG, id);
            if (value == null) {
                return null;
            }
            return parser.parseRollupTask(value.getValue());
        } catch (final DatastoreException e) {
            throw new RollUpException("Failed to read roll-up task " + id, e);
        } catch (final BeanValidationException | QueryException e) {
            throw new RollUpException("Failed to read rollups from the keystore", e);
        }
    }

    @Override
    public Set<String> listIds()
            throws RollUpException {
        try {
            final Set<String> ids = new HashSet<>();
            final Iterable<String> keys = keyStore.listKeys(SERVICE, SERVICE_KEY_CONFIG);
            for (final String key : keys) {
                ids.add(key);
            }
            return ids;
        } catch (final DatastoreException e) {
            throw new RollUpException("Failed to read roll-up ids from the service keystore", e);
        }
    }

    @Override
    public long getLastModifiedTime()
            throws RollUpException {
        try {
            final Date lastModifiedTime = keyStore.getServiceKeyLastModifiedTime(SERVICE, SERVICE_KEY_CONFIG);
            if (lastModifiedTime != null) {
                return lastModifiedTime.getTime();
            }
            return 0L;
        } catch (final DatastoreException e) {
            throw new RollUpException("Could not read from service keystore", e);
        }
    }

    private void importFromOldFile()
            throws RollUpException, IOException, QueryException {
        final File oldFile = new File(OLD_FILENAME);
        if (oldFile.exists()) {
            final List<String> taskJson = FileUtils.readLines(oldFile, StandardCharsets.UTF_8);
            final List<RollupTask> tasks = new ArrayList<>();
            for (final String json : taskJson) {
                final RollupTask task = parser.parseRollupTask(json);
                if (task != null) {
                    tasks.add(task);
                }
            }
            write(tasks);
            final boolean deleted = oldFile.delete();
            if (!deleted) {
                throw new IOException("Could not delete imported roll-up task file " + oldFile.getAbsolutePath());
            }
            logger.info("Roll-up task successfully imported from old format into the new format into the datastore. Old file was removed " + oldFile.getAbsolutePath());
        }
    }
}
