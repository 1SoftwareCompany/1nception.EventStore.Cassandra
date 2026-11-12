using Cassandra;
using System.Threading;
using System.Threading.Tasks;

namespace One.Inception.EventStore.Cassandra
{
    public interface ICassandraProvider
    {
        string GetKeyspace();
        Task<ICluster> GetClusterAsync(CancellationToken cancellationToken = default);
        Task<ISession> GetSessionAsync(CancellationToken cancellationToken = default);

        Task<ISession> GetWipeSessionAsync(CancellationToken cancellationToken = default) => throw new System.NotSupportedException("This provider does not implement explicit wipe sessions.");
    }
}
