using Cassandra;
using System.Threading.Tasks;

namespace One.Inception.EventStore.Cassandra
{
    public interface ICassandraProvider
    {
        string GetKeyspace();
        Task<ICluster> GetClusterAsync();
        Task<ISession> GetSessionAsync();
    }
}
