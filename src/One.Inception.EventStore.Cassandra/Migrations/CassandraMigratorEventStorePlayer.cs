using One.Inception.MessageProcessing;
using One.Inception.Migrations;
using Microsoft.Extensions.Logging;

namespace One.Inception.EventStore.Cassandra.Migrations
{
    public class CassandraMigratorEventStorePlayer : CassandraEventStore<MigratorCassandraReplaySettings>, IMigrationEventStorePlayer
    {
        public CassandraMigratorEventStorePlayer(IInceptionContextAccessor contextAccessor, MigratorCassandraReplaySettings settings, IndexByEventTypeStore indexByEventTypeStore, ILogger<CassandraEventStore> logger) : base(contextAccessor, settings, indexByEventTypeStore, logger)
        {

        }
    }
}
