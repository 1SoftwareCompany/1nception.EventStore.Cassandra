﻿using One.Inception.EventStore.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Options;

namespace One.Inception.EventStore.Cassandra.Integration.Tests;

public class CassandraEventStoreSchemaTests
{
	private readonly CassandraFixture cassandraFixture;

	public CassandraEventStoreSchemaTests(CassandraFixture cassandraFixture)
	{
		this.cassandraFixture = cassandraFixture;
	}

	[Fact]
	public async Task CreateStorageWithNoTableNamingAsync()
	{
		var naming = new NoTableNamingStrategy();
		var replicatoinStrategy = new SimpleReplicationStrategy(1);
		var schema = new CassandraEventStoreSchemaFixture(cassandraFixture).GetEventStoreSchema(naming, replicatoinStrategy);

		await schema.CreateStorageAsync();

		var session = await cassandraFixture.GetSessionAsync();
		var cluster = await cassandraFixture.GetClusterAsync();
		var tables = cluster.Metadata.GetTables(session.Keyspace);

		Assert.Contains(naming.GetName(), tables);
		Assert.Contains("index_by_eventtype", tables);
		Assert.Contains("message_counter", tables);
	}

	[Fact]
	public async Task CreateStorageWithTablePerBoundedContextAsync()
	{
		var bc = new BoundedContext { Name = "tests" };
		var naming = new TablePerBoundedContext(new TablePerBoundedContextOptionsMonitorMock(bc));
		var replicatoinStrategy = new SimpleReplicationStrategy(1);
		var schema = new CassandraEventStoreSchemaFixture(cassandraFixture).GetEventStoreSchema(naming, replicatoinStrategy);

		await schema.CreateStorageAsync();

		var session = await cassandraFixture.GetSessionAsync();
		var cluster = await cassandraFixture.GetClusterAsync();
		var tables = cluster.Metadata.GetTables(session.Keyspace);

		Assert.Contains(naming.GetName(), tables);
		Assert.Contains("index_by_eventtype", tables);
		Assert.Contains("message_counter", tables);
	}
}

class TablePerBoundedContextOptionsMonitorMock : IOptionsMonitor<BoundedContext>
{
	private readonly BoundedContext boundedContext;

	public TablePerBoundedContextOptionsMonitorMock(BoundedContext boundedContext)
	{
		this.boundedContext = boundedContext;
	}

	public BoundedContext CurrentValue => boundedContext;

	public BoundedContext Get(string name)
	{
		return boundedContext;
	}

	public IDisposable OnChange(Action<BoundedContext, string> listener)
	{
		listener(boundedContext, null);
		return null;
	}
}
