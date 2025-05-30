﻿using System.Security.Cryptography;
using One.Inception.EventStore.Index;
using One.Inception.MessageProcessing;

namespace One.Inception.EventStore.Cassandra.Integration.Tests;

[EnsureEventStore]
public class IndexByEventTypeStoreTests :
    IClassFixture<CassandraEventStoreSchemaFixture>
{
    private readonly CassandraFixture cassandraFixture;
    private readonly CassandraEventStoreSchemaFixture schemaFixture;

    public IndexByEventTypeStoreTests(
        CassandraFixture cassandraFixture,
        CassandraEventStoreSchemaFixture schemaFixture)
    {
        this.cassandraFixture = cassandraFixture;
        this.schemaFixture = schemaFixture;
    }

    [Fact]
    public async Task ApendAsync()
    {
        var accessor = new ContextAccessorMock
        {
            Context = new InceptionContext("tests", new NullServiceProviderMock())
        };
        var index = new IndexByEventTypeStoreFixture(accessor, cassandraFixture).Index;

        var id = Guid.NewGuid().ToString();
        Memory<byte> arId = new byte[64];
        RandomNumberGenerator.Fill(arId.Span);
        var revision = 1;
        var position = 0;
        var timestamp = DateTime.UtcNow.ToFileTimeUtc();

        var record = new IndexRecord(id, arId, revision, position, timestamp);
        await index.ApendAsync(record);

        List<IndexRecord> results = new List<IndexRecord>();
        await foreach (var item in index.GetRecordsAsync(new EventStore.PlayerOptions() { EventTypeId = id, After = DateTimeOffset.UtcNow.AddDays(-1), Before = DateTimeOffset.UtcNow.AddDays(1) }).ConfigureAwait(false))
        {
            results.Add(item);
        }
        var row = Assert.Single(results);

        Assert.Equal(record.AggregateRootId.Span, results.First().AggregateRootId.Span);
        Assert.Equal(record.Revision, results.First().Revision);
        Assert.Equal(record.Position, results.First().Position);
        Assert.Equal(record.TimeStamp, results.First().TimeStamp);
    }

    //[Fact]
    //public void GetAsync() // This dowsn't work because of the partition id
    //{
    //    var record = indexRecordFixture.IndexRecord;
    //    var row = Assert.Single(index.GetAsync(record.Id));
    //    Assert.Equal(record.AggregateRootId, row.AggregateRootId);
    //    Assert.Equal(record.Revision, row.Revision);
    //    Assert.Equal(record.Position, row.Position);
    //    Assert.Equal(record.TimeStamp, row.TimeStamp);
    //}

    //[Fact]
    //public async Task GetCountAsync() // This dowsn't work because of the partition id
    //{
    //    var count = await index.GetCountAsync(indexRecordFixture.IndexRecord.Id);
    //    Assert.Equal(1, count);
    //}

    [Fact]
    public async Task DeleteAsync()
    {
        var accessor = new ContextAccessorMock
        {
            Context = new InceptionContext("tests", new NullServiceProviderMock())
        };
        var index = new IndexByEventTypeStoreFixture(accessor, cassandraFixture).Index;

        var id = Guid.NewGuid().ToString();
        Memory<byte> arId = new byte[64];
        RandomNumberGenerator.Fill(arId.Span);
        var revision = 1;
        var position = 0;
        var timestamp = DateTime.UtcNow.ToFileTimeUtc();

        var record = new IndexRecord(id, arId, revision, position, timestamp);
        await index.ApendAsync(record);
        Exception exception = null;
        try
        {
            await index.DeleteAsync(record);
        }
        catch (Exception ex)
        {
            exception = ex;
        }

        List<IndexRecord> results = new List<IndexRecord>();
        var playerOptions = new EventStore.PlayerOptions()
        {
            EventTypeId = id,
            After = DateTimeOffset.UtcNow.AddDays(-1),
            Before = DateTimeOffset.UtcNow.AddDays(1)
        };
        await foreach (var item in index.GetRecordsAsync(playerOptions).ConfigureAwait(false))
        {
            results.Add(item);
        }

        Assert.Null(exception);
        Assert.Empty(results);
    }
}
