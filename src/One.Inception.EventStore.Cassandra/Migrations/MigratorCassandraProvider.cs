﻿using Cassandra;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace One.Inception.EventStore.Cassandra.Migrations
{
    public class MigratorCassandraProvider : CassandraProvider
    {
        public MigratorCassandraProvider(IOptionsMonitor<MigrationCassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ILogger<CassandraProvider> logger, IInitializer initializer = null)
            : base(optionsMonitor, keyspaceNamingStrategy, logger, initializer) { }
    }
}
