using One.Inception.MessageProcessing;
using Microsoft.Extensions.Logging.Abstractions;

namespace One.Inception.EventStore.Cassandra.Integration.Tests;

[EnsureEventStore]
public class LoadLargeAggregateCancellation
{
    private const int EventsCount = 6000; // more than the 5000 rows of the driver's default page size

    private static Task<ReadOnlyMemory<byte>> seeded;
    private readonly CassandraFixture cassandraFixture;

    public LoadLargeAggregateCancellation(CassandraFixture cassandraFixture)
    {
        this.cassandraFixture = cassandraFixture;
    }

    [Fact]
    public async Task LoadsEveryEventAcrossPagesWhenNotCanceled()
    {
        var arId = await SeedAsync();
        var store = CassandraEventStoreCancellation.CreateStore(cassandraFixture, new SerializerMock(), CancellationToken.None);

        var stream = await store.LoadAsync(new TestId(arId));

        Assert.Equal(Enumerable.Range(1, EventsCount), stream.Commits.Select(commit => commit.Revision));
    }

    [Theory]
    [InlineData(100)] // first page
    [InlineData(5500)] // second page
    public async Task StopsAtTheRowWhereItWasCanceled(int cancelAt)
    {
        var arId = await SeedAsync();
        using var cancellation = new CancellationTokenSource();
        var serializer = new CancelingSerializer(cancelAt, cancellation);
        var store = CassandraEventStoreCancellation.CreateStore(cassandraFixture, serializer, cancellation.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.LoadAsync(new TestId(arId)));

        Assert.Equal(cancelAt, serializer.Deserialized);
    }

    [Fact]
    public async Task DoesNotReadRowsWhenAlreadyCanceled()
    {
        var arId = await SeedAsync();
        var serializer = new CancelingSerializer(int.MaxValue, null);
        var store = CassandraEventStoreCancellation.CreateStore(cassandraFixture, serializer, new CancellationToken(true));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.LoadAsync(new TestId(arId)));

        Assert.Equal(0, serializer.Deserialized);
    }

    private Task<ReadOnlyMemory<byte>> SeedAsync() => seeded ??= InsertEventsAsync(); // xunit runs the tests of a class one by one

    private async Task<ReadOnlyMemory<byte>> InsertEventsAsync()
    {
        var arId = new BlobIdFixture().New();
        var store = CassandraEventStoreCancellation.CreateStore(cassandraFixture, new SerializerMock(), CancellationToken.None);

        await Parallel.ForEachAsync(Enumerable.Range(1, EventsCount), new ParallelOptions { MaxDegreeOfParallelism = 32 }, async (revision, _) =>
            await store.AppendAsync(new AggregateCommit(arId, revision, [new TestEvent(DateTimeOffset.UtcNow)], [], DateTime.UtcNow.ToFileTimeUtc())));

        return arId;
    }
}

[EnsureEventStore]
public class EnumerateEventStoreCancellation
{
    private readonly CassandraFixture cassandraFixture;

    public EnumerateEventStoreCancellation(CassandraFixture cassandraFixture)
    {
        this.cassandraFixture = cassandraFixture;
    }

    [Fact]
    public async Task DoesNotFinishOrSaveProgressOrPassIncompleteAggregate()
    {
        var arId = new BlobIdFixture().New();
        var store = CassandraEventStoreCancellation.CreateStore(cassandraFixture, new SerializerMock(), CancellationToken.None);
        await store.AppendAsync(new AggregateCommit(arId, 1, [new TestEvent(DateTimeOffset.UtcNow)], [], DateTime.UtcNow.ToFileTimeUtc()));
        await store.AppendAsync(new AggregateCommit(arId, 2, [new TestEvent(DateTimeOffset.UtcNow)], [], DateTime.UtcNow.ToFileTimeUtc()));

        using var cancellation = new CancellationTokenSource();
        var loaded = 0;
        var streams = 0;
        var progressNotifications = 0;
        var finished = false;

        await store.EnumerateEventStore(new PlayerOperator
        {
            OnLoadAsync = _ =>
            {
                loaded++;
                cancellation.Cancel();
                return Task.CompletedTask;
            },
            OnAggregateStreamLoadedAsync = _ =>
            {
                streams++;
                return Task.CompletedTask;
            },
            NotifyProgressAsync = _ =>
            {
                progressNotifications++;
                return Task.CompletedTask;
            },
            OnFinish = () =>
            {
                finished = true;
                return Task.CompletedTask;
            }
        }, new PlayerOptions(), cancellation.Token);

        Assert.Equal(1, loaded);
        Assert.Equal(0, streams);
        Assert.Equal(0, progressNotifications);
        Assert.False(finished);
    }
}

static class CassandraEventStoreCancellation
{
    public static CassandraEventStore CreateStore(CassandraFixture cassandraFixture, ISerializer serializer, CancellationToken cancellationToken)
    {
        var context = new InceptionContext("tests", new NullServiceProviderMock());
        typeof(InceptionContext).GetProperty(nameof(InceptionContext.CancellationToken)).SetValue(context, cancellationToken); // the setter is internal to 1nception

        var accessor = new ContextAccessorMock { Context = context };
        var index = new IndexByEventTypeStore(accessor, cassandraFixture, NullLogger<IndexByEventTypeStore>.Instance);

        return new CassandraEventStore(accessor, cassandraFixture, new NoTableNamingStrategy(), serializer, index, NullLogger<CassandraEventStore>.Instance);
    }
}

sealed class CancelingSerializer : ISerializer
{
    private readonly SerializerMock serializer = new SerializerMock();
    private readonly int cancelAt;
    private readonly CancellationTokenSource cancellation;

    public CancelingSerializer(int cancelAt, CancellationTokenSource cancellation)
    {
        this.cancelAt = cancelAt;
        this.cancellation = cancellation;
    }

    public int Deserialized { get; private set; }

    public T DeserializeFromBytes<T>(byte[] bytes)
    {
        if (++Deserialized == cancelAt)
            cancellation.Cancel();

        return serializer.DeserializeFromBytes<T>(bytes);
    }

    public byte[] SerializeToBytes<T>(T message) => serializer.SerializeToBytes(message);

    public string SerializeToString<T>(T message) => serializer.SerializeToString(message);
}
