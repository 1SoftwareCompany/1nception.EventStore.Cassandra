using One.Inception.MessageProcessing;
using Microsoft.Extensions.Logging.Abstractions;

namespace One.Inception.EventStore.Cassandra.Integration.Tests;

public class IndexByEventTypeStoreFixture
{
    public IndexByEventTypeStoreFixture(IInceptionContextAccessor contextAccessor, CassandraFixture cassandraFixture)
    {
        Index = new IndexByEventTypeStore(contextAccessor, cassandraFixture, NullLogger<IndexByEventTypeStore>.Instance);
    }

    public IndexByEventTypeStore Index { get; }
}
