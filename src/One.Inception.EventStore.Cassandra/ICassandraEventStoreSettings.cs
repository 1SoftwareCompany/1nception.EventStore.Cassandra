namespace One.Inception.EventStore.Cassandra
{
    public interface ICassandraEventStoreSettings
    {
        ICassandraProvider CassandraProvider { get; }
        ISerializer Serializer { get; }
        ITableNamingStrategy TableNameStrategy { get; }
    }
}
