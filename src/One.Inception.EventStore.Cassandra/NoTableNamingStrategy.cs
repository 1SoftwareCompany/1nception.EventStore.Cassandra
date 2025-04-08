namespace One.Inception.EventStore.Cassandra
{
    public sealed class NoTableNamingStrategy : ITableNamingStrategy
    {
        public string GetName()
        {
            return "events";
        }
    }
}
