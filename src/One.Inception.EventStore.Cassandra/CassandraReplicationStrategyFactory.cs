using System;
using System.Collections.Generic;
using One.Inception.EventStore.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Options;

namespace One.Inception.EventStore.Cassandra
{
    class CassandraReplicationStrategyFactory
    {
        private readonly CassandraProviderOptions options;

        public CassandraReplicationStrategyFactory(IOptionsMonitor<CassandraProviderOptions> optionsMonitor)
        {
            this.options = optionsMonitor.CurrentValue;
        }

        internal ICassandraReplicationStrategy GetReplicationStrategy()
        {
            ICassandraReplicationStrategy replicationStrategy = null;
            if (options.ReplicationStrategy.Equals("simple", StringComparison.OrdinalIgnoreCase))
            {
                replicationStrategy = new SimpleReplicationStrategy(options.ReplicationFactor);
            }
            else if (options.ReplicationStrategy.Equals("network_topology", StringComparison.OrdinalIgnoreCase))
            {
                var settings = new List<NetworkTopologyReplicationStrategy.DataCenterSettings>();
                foreach (var datacenter in options.Datacenters)
                {
                    var setting = new NetworkTopologyReplicationStrategy.DataCenterSettings(datacenter, options.ReplicationFactor);
                    settings.Add(setting);
                }
                replicationStrategy = new NetworkTopologyReplicationStrategy(settings);
            }

            return replicationStrategy;
        }
    }
}
