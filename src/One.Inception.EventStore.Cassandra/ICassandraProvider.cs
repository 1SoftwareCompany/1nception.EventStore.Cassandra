using Cassandra;
using System.Threading.Tasks;

namespace One.Inception.EventStore.Cassandra
{
    public interface ICassandraProvider
    {
        string GetKeyspace();
        Task<ICluster> GetClusterAsync();
        Task<ISession> GetSessionAsync();

        Task<ISession> GetWipeSessionAsync() => throw new System.NotSupportedException("This provider does not implement explicit wipe sessions.");
    }
}
