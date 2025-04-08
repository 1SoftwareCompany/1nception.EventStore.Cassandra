namespace One.Inception.EventStore.Cassandra.ReplicationStrategies
{
    public interface ICassandraReplicationStrategy
    {
        string CreateKeySpaceTemplate(string keySpace);
    }
}
