using One.Inception.EventStore.Cassandra.ReplicationStrategies;

namespace One.Inception.EventStore.Cassandra.Integration.Tests;

public class CassandraEventStoreSchemaFixture
{
    private readonly CassandraFixture cassandraFixture;

    public CassandraEventStoreSchemaFixture(CassandraFixture cassandraFixture)
    {
        this.cassandraFixture = cassandraFixture;
    }

    public CassandraEventStoreSchema GetEventStoreSchema(ITableNamingStrategy namingStrategy, ICassandraReplicationStrategy replicationStrategy)
    {
        return new CassandraEventStoreSchema(cassandraFixture, namingStrategy, replicationStrategy);
    }
}
