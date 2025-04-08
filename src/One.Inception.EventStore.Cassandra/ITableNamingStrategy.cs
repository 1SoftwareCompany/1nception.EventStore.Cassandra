namespace One.Inception.EventStore.Cassandra
{
    public interface ITableNamingStrategy
    {
        string GetName();
    }
}
