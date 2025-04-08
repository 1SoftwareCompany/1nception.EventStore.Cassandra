namespace One.Inception.EventStore.Cassandra
{
    public interface IKeyspaceNamingStrategy
    {
        string GetName(string baseConfigurationKeyspace);
    }
}
