/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.connection;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientException;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.event.ClusterListener;
import com.mongodb.internal.selector.LatencyMinimizingServerSelector;
import com.mongodb.selector.CompositeServerSelector;
import com.mongodb.selector.ServerSelector;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ServerAddressHelper.createServerAddress;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Settings for the cluster.
 *
 * @since 3.0
 */
@Immutable
public final class ClusterSettings {
    private final String srvHost;
    private final List<ServerAddress> hosts;
    private final ClusterConnectionMode mode;
    private final ClusterType requiredClusterType;
    private final String requiredReplicaSetName;
    private final ServerSelector serverSelector;
    private final long localThresholdMS;
    private final long serverSelectionTimeoutMS;
    private final int maxWaitQueueSize;
    private final List<ClusterListener> clusterListeners;

    /**
     * Get a builder for this class.
     *
     * @return a new Builder for creating ClusterSettings.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder instance.
     *
     * @param clusterSettings existing ClusterSettings to default the builder settings on.
     * @return a builder
     * @since 3.5
     */
    public static Builder builder(final ClusterSettings clusterSettings) {
        return builder().applySettings(clusterSettings);
    }

    /**
     * A builder for the cluster settings.
     */
    @NotThreadSafe
    public static final class Builder {
        private static final List<ServerAddress> DEFAULT_HOSTS = singletonList(new ServerAddress());
        private String srvHost;
        private List<ServerAddress> hosts = DEFAULT_HOSTS;
        private ClusterConnectionMode mode;
        private ClusterType requiredClusterType = ClusterType.UNKNOWN;
        private String requiredReplicaSetName;
        private ServerSelector serverSelector;
        private long serverSelectionTimeoutMS = MILLISECONDS.convert(30, TimeUnit.SECONDS);
        private long localThresholdMS = MILLISECONDS.convert(15, MILLISECONDS);
        private int maxWaitQueueSize = 500;
        private List<ClusterListener> clusterListeners = new ArrayList<ClusterListener>();

        private Builder() {
        }

        /**
         * Applies the clusterSettings to the builder
         *
         * <p>Note: Overwrites all existing settings</p>
         *
         * @param clusterSettings the clusterSettings
         * @return this
         * @since 3.7
         */
        public Builder applySettings(final ClusterSettings clusterSettings) {
            notNull("clusterSettings", clusterSettings);
            srvHost = clusterSettings.srvHost;
            hosts = clusterSettings.hosts;
            mode = clusterSettings.mode;
            requiredReplicaSetName = clusterSettings.requiredReplicaSetName;
            requiredClusterType = clusterSettings.requiredClusterType;
            localThresholdMS = clusterSettings.localThresholdMS;
            serverSelectionTimeoutMS = clusterSettings.serverSelectionTimeoutMS;
            maxWaitQueueSize = clusterSettings.maxWaitQueueSize;
            clusterListeners = new ArrayList<ClusterListener>(clusterSettings.clusterListeners);
            serverSelector = unpackServerSelector(clusterSettings.serverSelector);
            return this;
        }

        /**
         * Sets the host name to use in order to look up an SRV DNS record to find the MongoDB hosts.
         *
         * @param srvHost the SRV host name
         * @return this
         */
        public Builder srvHost(final String srvHost) {
            if (this.hosts != DEFAULT_HOSTS) {
                throw new IllegalArgumentException("Can not set both hosts and srvHost");
            }
            this.srvHost = srvHost;
            return this;
        }

        /**
         * Sets the hosts for the cluster. Any duplicate server addresses are removed from the list.
         *
         * @param hosts the seed list of hosts
         * @return this
         */
        public Builder hosts(final List<ServerAddress> hosts) {
            notNull("hosts", hosts);
            if (hosts.isEmpty()) {
                throw new IllegalArgumentException("hosts list may not be empty");
            }
            if (srvHost != null) {
                throw new IllegalArgumentException("srvHost must be null");
            }
            Set<ServerAddress> hostsSet = new LinkedHashSet<ServerAddress>(hosts.size());
            for (ServerAddress serverAddress : hosts) {
                notNull("serverAddress", serverAddress);
                hostsSet.add(createServerAddress(serverAddress.getHost(), serverAddress.getPort()));
            }
            this.hosts = unmodifiableList(new ArrayList<ServerAddress>(hostsSet));
            return this;
        }

        /**
         * Sets the mode for this cluster.
         *
         * @param mode the cluster connection mode
         * @return this;
         */
        public Builder mode(final ClusterConnectionMode mode) {
            this.mode = notNull("mode", mode);
            return this;
        }

        /**
         * Sets the required replica set name for the cluster.
         *
         * @param requiredReplicaSetName the required replica set name.
         * @return this
         */
        public Builder requiredReplicaSetName(final String requiredReplicaSetName) {
            this.requiredReplicaSetName = requiredReplicaSetName;
            return this;
        }

        /**
         * Sets the required cluster type for the cluster.
         *
         * @param requiredClusterType the required cluster type
         * @return this
         */
        public Builder requiredClusterType(final ClusterType requiredClusterType) {
            this.requiredClusterType = notNull("requiredClusterType", requiredClusterType);
            return this;
        }

        /**
         * Sets the local threshold.
         *
         * @param localThreshold the acceptable latency difference, in milliseconds, which must be &gt;= 0
         * @param timeUnit the time unit
         * @throws IllegalArgumentException if {@code localThreshold < 0}
         * @return this
         * @since 3.7
         */
        public Builder localThreshold(final long localThreshold, final TimeUnit timeUnit) {
            isTrueArgument("localThreshold must be >= 0", localThreshold >= 0);
            this.localThresholdMS = MILLISECONDS.convert(localThreshold, timeUnit);
            return this;
        }

        /**
         * Adds a server selector for the cluster to apply before selecting a server.
         *
         * @param serverSelector the server selector to apply as selector.
         * @return this
         * @see #getServerSelector()
         */
        public Builder serverSelector(final ServerSelector serverSelector) {
            this.serverSelector = serverSelector;
            return this;
        }

        /**
         * Sets the timeout to apply when selecting a server.  If the timeout expires before a server is found to handle a request, a
         * {@link com.mongodb.MongoTimeoutException} will be thrown.  The default value is 30 seconds.
         *
         * <p> A value of 0 means that it will timeout immediately if no server is available.  A negative value means to wait
         * indefinitely.</p>
         *
         * @param serverSelectionTimeout the timeout
         * @param timeUnit the time unit
         * @return this
         */
        public Builder serverSelectionTimeout(final long serverSelectionTimeout, final TimeUnit timeUnit) {
            this.serverSelectionTimeoutMS = MILLISECONDS.convert(serverSelectionTimeout, timeUnit);
            return this;
        }

        /**
         * <p>This is the maximum number of concurrent operations allowed to wait for a server to become available. All further operations
         * will get an exception immediately.</p>
         *
         * <p>Default is 500.</p>
         *
         * @param maxWaitQueueSize the number of threads that are allowed to be waiting for a connection.
         * @return this
         * @deprecated in the next major release, wait queue size limitations will be removed
         */
        @Deprecated
        public Builder maxWaitQueueSize(final int maxWaitQueueSize) {
            this.maxWaitQueueSize = maxWaitQueueSize;
            return this;
        }

        /**
         * Adds a cluster listener.
         *
         * @param clusterListener the non-null cluster listener
         * @return this
         * @since 3.3
         */
        public Builder addClusterListener(final ClusterListener clusterListener) {
            notNull("clusterListener", clusterListener);
            clusterListeners.add(clusterListener);
            return this;
        }

        /**
         * Takes the settings from the given {@code ConnectionString} and applies them to the builder
         *
         * @param connectionString the connection string containing details of how to connect to MongoDB
         * @return this
         */
        public Builder applyConnectionString(final ConnectionString connectionString) {
            if (connectionString.isSrvProtocol()) {
                mode(ClusterConnectionMode.MULTIPLE);
                srvHost(connectionString.getHosts().get(0));
            }
            else if (connectionString.getHosts().size() == 1 && connectionString.getRequiredReplicaSetName() == null) {
                mode(ClusterConnectionMode.SINGLE)
                .hosts(singletonList(createServerAddress(connectionString.getHosts().get(0))));
            } else {
                List<ServerAddress> seedList = new ArrayList<ServerAddress>();
                for (final String cur : connectionString.getHosts()) {
                    seedList.add(createServerAddress(cur));
                }
                mode(ClusterConnectionMode.MULTIPLE).hosts(seedList);
            }
            requiredReplicaSetName(connectionString.getRequiredReplicaSetName());

            Integer maxConnectionPoolSize = connectionString.getMaxConnectionPoolSize();
            int maxSize = maxConnectionPoolSize != null ? maxConnectionPoolSize : 100;

            Integer threadsAllowedToBlockForConnectionMultiplier = connectionString.getThreadsAllowedToBlockForConnectionMultiplier();
            int waitQueueMultiple = threadsAllowedToBlockForConnectionMultiplier != null
                                    ? threadsAllowedToBlockForConnectionMultiplier : 5;
            maxWaitQueueSize(waitQueueMultiple * maxSize);

            Integer serverSelectionTimeout = connectionString.getServerSelectionTimeout();
            if (serverSelectionTimeout != null) {
                serverSelectionTimeout(serverSelectionTimeout, MILLISECONDS);
            }

            Integer localThreshold = connectionString.getLocalThreshold();
            if (localThreshold != null) {
                localThreshold(localThreshold, MILLISECONDS);
            }
            return this;
        }

        private ServerSelector unpackServerSelector(final ServerSelector serverSelector) {
            if (serverSelector instanceof CompositeServerSelector) {
                return ((CompositeServerSelector) serverSelector).getServerSelectors().get(0);
            }
            return null;
        }

        private ServerSelector packServerSelector() {
            ServerSelector latencyMinimizingServerSelector = new LatencyMinimizingServerSelector(localThresholdMS, MILLISECONDS);
            if (serverSelector == null) {
                return latencyMinimizingServerSelector;
            }
            return new CompositeServerSelector(asList(serverSelector, latencyMinimizingServerSelector));
        }

        /**
         * Build the settings from the builder.
         *
         * @return the cluster settings
         */
        public ClusterSettings build() {
            return new ClusterSettings(this);
        }
    }

    /**
     * Gets the host name from which to lookup SRV record for the seed list
     * @return the SRV host, or null if none specified
     * @since 3.10
     */
    public String getSrvHost() {
        return srvHost;
    }

    /**
     * Gets the seed list of hosts for the cluster.
     *
     * @return the seed list of hosts
     */
    public List<ServerAddress> getHosts() {
        return hosts;
    }

    /**
     * Gets the mode.
     *
     * @return the mode
     */
    public ClusterConnectionMode getMode() {
        return mode;
    }

    /**
     * Gets the required cluster type
     *
     * @return the required cluster type
     */
    public ClusterType getRequiredClusterType() {
        return requiredClusterType;
    }

    /**
     * Gets the required replica set name.
     *
     * @return the required replica set name
     */
    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }

    /**
     * Gets the server selector.
     *
     * <p>The server selector augments the normal server selection rules applied by the driver when determining
     * which server to send an operation to.  At the point that it's called by the driver, the
     * {@link com.mongodb.connection.ClusterDescription} which is passed to it contains a list of
     * {@link com.mongodb.connection.ServerDescription} instances which satisfy either the configured {@link com.mongodb.ReadPreference}
     * for any read operation or ones that can take writes (e.g. a standalone, mongos, or replica set primary).
     * </p>
     * <p>The server selector can then filter the {@code ServerDescription} list using whatever criteria that is required by the
     * application.</p>
     * <p>After this selector executes, two additional selectors are applied by the driver:</p>
     * <ul>
     * <li>select from within the latency window</li>
     * <li>select a random server from those remaining</li>
     * </ul>
     * <p>To skip the latency window selector, an application can:</p>
     * <ul>
     * <li>configure the local threshold to a sufficiently high value so that it doesn't exclude any servers</li>
     * <li>return a list containing a single server from this selector (which will also make the random member selector a no-op)</li>
     * </ul>
     *
     * @return the server selector, which may be null
     * @see Cluster#selectServer(com.mongodb.selector.ServerSelector)
     */
    public ServerSelector getServerSelector() {
        return serverSelector;
    }

    /**
     * Gets the timeout to apply when selecting a server.  If the timeout expires before a server is found to
     * handle a request, a {@link com.mongodb.MongoTimeoutException} will be thrown.  The default value is 30 seconds.
     *
     * <p> A value of 0 means that it will timeout immediately if no server is available.  A negative value means to wait
     * indefinitely.</p>
     *
     * @param timeUnit the time unit
     * @return the timeout in the given time unit
     */
    public long getServerSelectionTimeout(final TimeUnit timeUnit) {
        return timeUnit.convert(serverSelectionTimeoutMS, MILLISECONDS);
    }

    /**
     * Gets the local threshold.  When choosing among multiple MongoDB servers to send a request, the MongoClient will only
     * send that request to a server whose ping time is less than or equal to the server with the fastest ping time plus the local
     * threshold.
     *
     * <p>For example, let's say that the client is choosing a server to send a query when the read preference is {@code
     * ReadPreference.secondary()}, and that there are three secondaries, server1, server2, and server3, whose ping times are 10, 15, and 16
     * milliseconds, respectively.  With a local threshold of 5 milliseconds, the client will send the query to either
     * server1 or server2 (randomly selecting between the two).
     * </p>
     *
     * <p>Default is 15 milliseconds.</p>
     *
     * @param timeUnit the time unit
     * @return the local threshold in the given timeunit.
     * @since 3.7
     * @mongodb.driver.manual reference/program/mongos/#cmdoption--localThreshold Local Threshold
     */
    public long getLocalThreshold(final TimeUnit timeUnit) {
        return timeUnit.convert(localThresholdMS, MILLISECONDS);
    }

    /**
     * <p>This is the maximum number of threads that may be waiting for a connection to become available from the pool. All further threads
     * will get an exception immediately.</p>
     *
     * <p>Default is 500.</p>
     *
     * @return the number of threads that are allowed to be waiting for a connection.
     * @deprecated in the next major release, wait queue size limitations will be removed
     */
    @Deprecated
    public int getMaxWaitQueueSize() {
        return maxWaitQueueSize;
    }

    /**
     * Gets the cluster listeners.  The default value is an empty list.
     *
     * @return the cluster listeners
     * @since 3.3
     */
    public List<ClusterListener> getClusterListeners() {
        return clusterListeners;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClusterSettings that = (ClusterSettings) o;

        if (maxWaitQueueSize != that.maxWaitQueueSize) {
            return false;
        }
        if (serverSelectionTimeoutMS != that.serverSelectionTimeoutMS) {
            return false;
        }
        if (localThresholdMS != that.localThresholdMS) {
            return false;
        }
        if (srvHost != null ? !srvHost.equals(that.srvHost) : that.srvHost != null) {
            return false;
        }
        if (!hosts.equals(that.hosts)) {
            return false;
        }
        if (mode != that.mode) {
            return false;
        }
        if (requiredClusterType != that.requiredClusterType) {
            return false;
        }
        if (requiredReplicaSetName != null ? !requiredReplicaSetName.equals(that.requiredReplicaSetName)
                    : that.requiredReplicaSetName != null) {
            return false;
        }
        if (serverSelector != null ? !serverSelector.equals(that.serverSelector) : that.serverSelector != null) {
            return false;
        }
        if (!clusterListeners.equals(that.clusterListeners)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = hosts.hashCode();
        result = 31 * result + (srvHost != null ? srvHost.hashCode() : 0);
        result = 31 * result + mode.hashCode();
        result = 31 * result + requiredClusterType.hashCode();
        result = 31 * result + (requiredReplicaSetName != null ? requiredReplicaSetName.hashCode() : 0);
        result = 31 * result + (serverSelector != null ? serverSelector.hashCode() : 0);
        result = 31 * result + (int) (serverSelectionTimeoutMS ^ (serverSelectionTimeoutMS >>> 32));
        result = 31 * result + (int) (localThresholdMS ^ (localThresholdMS >>> 32));
        result = 31 * result + maxWaitQueueSize;
        result = 31 * result + clusterListeners.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "{"
               + (hosts.isEmpty() ? "" : "hosts=" + hosts)
               + (srvHost == null ? "" : ", srvHost=" + srvHost)
               + ", mode=" + mode
               + ", requiredClusterType=" + requiredClusterType
               + ", requiredReplicaSetName='" + requiredReplicaSetName + '\''
               + ", serverSelector='" + serverSelector + '\''
               + ", clusterListeners='" + clusterListeners + '\''
               + ", serverSelectionTimeout='" + serverSelectionTimeoutMS + " ms" + '\''
               + ", localThreshold='" + serverSelectionTimeoutMS + " ms" + '\''
               + ", maxWaitQueueSize=" + maxWaitQueueSize
               + '}';
    }

    /**
     * Returns a short, pretty description for these ClusterSettings.
     *
     * @return a String description of the relevant settings.
     */
    public String getShortDescription() {
        return "{"
                + (hosts.isEmpty() ? "" : "hosts=" + hosts)
                + (srvHost == null ? "" : ", srvHost=" + srvHost)
               + ", mode=" + mode
               + ", requiredClusterType=" + requiredClusterType
               + ", serverSelectionTimeout='" + serverSelectionTimeoutMS + " ms" + '\''
               + ", maxWaitQueueSize=" + maxWaitQueueSize
               + (requiredReplicaSetName == null ? "" : ", requiredReplicaSetName='" + requiredReplicaSetName + '\'')
               + '}';
    }

    private ClusterSettings(final Builder builder) {
        // TODO: Unit test this
        if (builder.srvHost != null) {
            if (builder.srvHost.contains(":")) {
                throw new IllegalArgumentException("The srvHost can not contain a host name that specifies a port");
            }

            if (builder.hosts.get(0).getHost().split("\\.").length < 3) {
                throw new MongoClientException(format("An SRV host name '%s' was provided that does not contain at least three parts. "
                        + "It must contain a hostname, domain name and a top level domain.", builder.hosts.get(0).getHost()));
            }
        }

        if (builder.hosts.size() > 1 && builder.requiredClusterType == ClusterType.STANDALONE) {
            throw new IllegalArgumentException("Multiple hosts cannot be specified when using ClusterType.STANDALONE.");
        }

        if (builder.mode != null && builder.mode == ClusterConnectionMode.SINGLE && builder.hosts.size() > 1) {
            throw new IllegalArgumentException("Can not directly connect to more than one server");
        }

        if (builder.requiredReplicaSetName != null) {
            if (builder.requiredClusterType == ClusterType.UNKNOWN) {
                builder.requiredClusterType = ClusterType.REPLICA_SET;
            } else if (builder.requiredClusterType != ClusterType.REPLICA_SET) {
                throw new IllegalArgumentException("When specifying a replica set name, only ClusterType.UNKNOWN and "
                                                   + "ClusterType.REPLICA_SET are valid.");
            }
        }

        srvHost = builder.srvHost;
        hosts = builder.hosts;
        mode = builder.mode != null ? builder.mode : hosts.size() == 1 ? ClusterConnectionMode.SINGLE : ClusterConnectionMode.MULTIPLE;
        requiredReplicaSetName = builder.requiredReplicaSetName;
        requiredClusterType = builder.requiredClusterType;
        localThresholdMS = builder.localThresholdMS;
        serverSelector = builder.packServerSelector();
        serverSelectionTimeoutMS = builder.serverSelectionTimeoutMS;
        maxWaitQueueSize = builder.maxWaitQueueSize;
        clusterListeners = unmodifiableList(builder.clusterListeners);
    }
}
