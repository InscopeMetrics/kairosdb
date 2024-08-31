package org.kairosdb.datastore.cassandra;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;


/**
 * This class holds two different load balancing policies.  One for queries and one
 * for ingesting data.  This then returns a query plan based on if the statement is a
 * batch statement (for ingest) or not.
 * <p>
 * The purpose is so inserts will not shuffle replicas so batching can be done efficiently
 * but queries will shuffle replicas
 */
public class SelectiveLoadBalancingPolicy implements LoadBalancingPolicy {
    private final LoadBalancingPolicy m_queryPolicy;
    private final LoadBalancingPolicy m_writePolicy;

    public SelectiveLoadBalancingPolicy(final LoadBalancingPolicy queryPolicy, final LoadBalancingPolicy writePolicy) {
        m_queryPolicy = queryPolicy;
        m_writePolicy = writePolicy;
    }

    @Override
    public void init(final Map<UUID, Node> nodes, final DistanceReporter distanceReporter) {
        m_queryPolicy.init(nodes, distanceReporter);
        m_writePolicy.init(nodes, distanceReporter);
    }

    @Override
    @NonNull
    public Queue<Node> newQueryPlan(final Request request, final Session session) {
        if (session instanceof BatchStatement) {
            return m_writePolicy.newQueryPlan(request, session);
        } else {
            return m_queryPolicy.newQueryPlan(request, session);
        }
    }

    @Override
    public void onAdd(final Node node) {
        m_queryPolicy.onAdd(node);
        m_writePolicy.onAdd(node);
    }

    @Override
    public void onUp(final Node node) {
        m_queryPolicy.onUp(node);
        m_writePolicy.onUp(node);
    }

    @Override
    public void onDown(final Node node) {
        m_queryPolicy.onDown(node);
        m_writePolicy.onDown(node);
    }

    @Override
    public void onRemove(final Node node) {
        m_queryPolicy.onRemove(node);
        m_writePolicy.onRemove(node);
    }

    @Override
    public void close() {
        m_queryPolicy.close();
        m_writePolicy.close();
    }
}
