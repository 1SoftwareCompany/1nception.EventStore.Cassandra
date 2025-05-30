﻿using System.Runtime.Serialization;
using Cassandra;
using One.Inception.MessageProcessing;
using One.Inception.EventStore.Cassandra.Counters;
using Microsoft.Extensions.Logging.Abstractions;

namespace One.Inception.EventStore.Cassandra.Integration.Tests;

[EnsureEventStore]
public class MessageCounterTests
{
    private readonly CassandraFixture cassandraFixture;

    public MessageCounterTests(CassandraFixture cassandraFixture)
    {
        this.cassandraFixture = cassandraFixture;
    }

    [Fact]
    public async Task IncrementAsync()
    {
        var accessor = new ContextAccessorMock
        {
            Context = new InceptionContext("tests", new NullServiceProviderMock())
        };
        var counter = new MessageCounter(accessor, cassandraFixture, NullLogger<MessageCounter>.Instance);
        var countedType = typeof(IncrementCounted);
        var contractId = countedType.GetContractId();

        await counter.IncrementAsync(countedType);

        var session = await cassandraFixture.GetSessionAsync();
        var statement = new SimpleStatement("""SELECT * FROM "message_counter" WHERE msgid=?;""", contractId);
        var rows = await session.ExecuteAsync(statement);

        var row = Assert.Single(rows);
        Assert.Equal(1L, row.GetValue<long>("cv"));
        Assert.Equal(contractId, row.GetValue<string>("msgid"));
    }

    [Fact]
    public async Task DecrementAsync()
    {
        var accessor = new ContextAccessorMock
        {
            Context = new InceptionContext("tests", new NullServiceProviderMock())
        };
        var counter = new MessageCounter(accessor, cassandraFixture, NullLogger<MessageCounter>.Instance);
        var countedType = typeof(DecrementCounted);
        var contractId = countedType.GetContractId();

        await counter.IncrementAsync(countedType, 5);
        await counter.DecrementAsync(countedType);

        var session = await cassandraFixture.GetSessionAsync();
        var statement = new SimpleStatement("""SELECT * FROM "message_counter" WHERE msgid=?;""", contractId);
        var rows = await session.ExecuteAsync(statement);

        var row = Assert.Single(rows);
        Assert.Equal(4L, row.GetValue<long>("cv"));
        Assert.Equal(contractId, row.GetValue<string>("msgid"));
    }

    [Fact]
    public async Task GetCountAsync()
    {
        var accessor = new ContextAccessorMock
        {
            Context = new InceptionContext("tests", new NullServiceProviderMock())
        };
        var counter = new MessageCounter(accessor, cassandraFixture, NullLogger<MessageCounter>.Instance);
        var countedType = typeof(GetCountCounted);
        var contractId = countedType.GetContractId();

        await counter.IncrementAsync(countedType, 5);
        var count = await counter.GetCountAsync(countedType);

        Assert.Equal(5L, count);
    }

    [Fact]
    public async Task ResetAsync()
    {
        var accessor = new ContextAccessorMock
        {
            Context = new InceptionContext("tests", new NullServiceProviderMock())
        };
        var counter = new MessageCounter(accessor, cassandraFixture, NullLogger<MessageCounter>.Instance);
        var countedType = typeof(ResetCounted);
        var contractId = countedType.GetContractId();

        await counter.IncrementAsync(countedType, 5);
        await counter.ResetAsync(countedType);

        var session = await cassandraFixture.GetSessionAsync();
        var statement = new SimpleStatement("""SELECT * FROM "message_counter" WHERE msgid=?;""", contractId);
        var rows = await session.ExecuteAsync(statement);

        var row = Assert.Single(rows);
        Assert.Equal(0L, row.GetValue<long>("cv"));
        Assert.Equal(contractId, row.GetValue<string>("msgid"));
    }
}

[DataContract(Name = "e0a2dc56-c0ce-4174-87a5-8bbd16d3af4e")] file class IncrementCounted { }
[DataContract(Name = "cc9fd718-6a84-4d36-98a1-97872f8da7ad")] file class DecrementCounted { }
[DataContract(Name = "c250cbd6-84ce-4439-b0f4-f8302161370a")] file class GetCountCounted { }
[DataContract(Name = "b64f1efd-dd1f-4d33-9651-ed74af1f002a")] file class ResetCounted { }
