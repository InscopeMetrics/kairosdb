package org.kairosdb.rollup;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.kairosdb.util.SummingMap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class ScoreBalancingAlgorithm implements BalancingAlgorithm {
    @Inject
    public ScoreBalancingAlgorithm() {
    }

    @Override
    public Map<String, String> rebalance(final Set<String> hosts, final Map<String, Long> scores) {
        final Map<String, String> balancedAssignments = new HashMap<>();
        final List<ServerAssignment> hostScores = new ArrayList<>();

        // Add all hosts
        for (final String host : hosts) {
            hostScores.add(new ServerAssignment(host));
        }

        // Make assignments. The host on top of the set has the smallest total score
        for (final String id : scores.keySet()) {
            final ServerAssignment leastLoaded = hostScores.get(0);
            leastLoaded.score += scores.get(id);
            balancedAssignments.put(id, leastLoaded.host);
            hostScores.sort(Comparator.comparingLong(o -> o.score));
        }

        return balancedAssignments;
    }

    @Override
    public Map<String, String> balance(final Set<String> hosts, final Map<String, String> currentAssignments, final Map<String, Long> scores) {
        final Map<String, String> assignments = new HashMap<>();

        final SummingMap totals = new SummingMap();

        for (final String id : currentAssignments.keySet()) {
            final String hostname = currentAssignments.get(id);
            final Long score = scores.get(id);
            if (score != null) {
                totals.put(hostname, score);
            }
        }

        // Add any hosts not in the currentAssignments
        for (final String host : hosts) {
            if (!totals.containsKey(host)) {
                totals.put(host, 0L);
            }
        }

        final Set<String> unassignedIds = Sets.difference(scores.keySet(), currentAssignments.keySet());
        for (final String unassignedId : unassignedIds) {
            final String host = totals.getKeyForSmallestValue();
            assignments.put(unassignedId, host);
            totals.put(host, scores.get(unassignedId));
        }

        return assignments;
    }

    public class ServerAssignment {
        public long score;
        public String host;

        ServerAssignment(final String host) {
            this.host = checkNotNullOrEmpty(host, "host cannot be null or empty");
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final ServerAssignment that = (ServerAssignment) o;

            return host.equals(that.host);
        }

        @Override
        public int hashCode() {
            return host.hashCode();
        }
    }
}
