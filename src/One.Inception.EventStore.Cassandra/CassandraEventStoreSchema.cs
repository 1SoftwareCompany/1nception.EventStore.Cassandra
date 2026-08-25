using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using One.Inception.EventStore.Cassandra.Counters;
using One.Inception.EventStore.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace One.Inception.EventStore.Cassandra
{
    public class CassandraEventStoreSchema // should be internal but the xunit prevents that. Why? :D
    {
        private static readonly ILogger logger = InceptionLogger.CreateLogger(typeof(CassandraEventStoreSchema));

        private const string CreateEventsTableQueryTemplate = @"CREATE TABLE IF NOT EXISTS {0}.{1} (id blob, ts bigint, rev int, pos int, data blob, PRIMARY KEY (id,rev,pos)) WITH CLUSTERING ORDER BY (rev ASC, pos ASC);";
        private const string CREATE_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS {0}.{1} (et text, pid int, aid blob, rev int, pos int, ts bigint, PRIMARY KEY ((et,pid),ts,aid,rev,pos)) WITH CLUSTERING ORDER BY (ts ASC);"; // ASC element required to be in second position in primary key https://stackoverflow.com/questions/23185331/cql-bad-request-missing-clustering-order-for-column
        private const string INDEX_BY_EVENT_TYPE_TABLE_NAME = "index_by_eventtype";
        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;
        private readonly ICassandraReplicationStrategy replicationStrategy;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();

        public CassandraEventStoreSchema(ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ICassandraReplicationStrategy replicationStrategy)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
            this.replicationStrategy = replicationStrategy;
        }

        /// <summary>
        /// This is the main method which the framework invokes. Other methods are also exposed, why not?!?
        /// </summary>
        /// <returns></returns>
        public async Task CreateStorageAsync()
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);

            await CreateKeyspace(session).ConfigureAwait(false);

            await CreateEventsStorageAsync(session).ConfigureAwait(false);

            await CreateIndeciesAsync(session).ConfigureAwait(false);
        }

        public async Task CreateKeyspace(ISession session)
        {
            long t0 = Stopwatch.GetTimestamp();

            IStatement createTableStatement = await GetCreateKeySpaceQuery(session).ConfigureAwait(false);
            var rs = await session.ExecuteAsync(createTableStatement).ConfigureAwait(false);

            TimeSpan elapsed = Stopwatch.GetElapsedTime(t0);
            logger.LogInformation("[EventStore] Created keyspace... Maybe?! Is schema in agreement = {isSchemaInAgreement}. Time elapsed : {timeForExecution}", rs?.Info?.IsSchemaInAgreement, elapsed);
        }

        public Task CreateEventsStorageAsync(ISession session)
        {
            string tableName = tableNameStrategy.GetName();
            return CreateTableAsync(session, CreateEventsTableQueryTemplate, tableName);
        }

        public async Task CreateIndeciesAsync(ISession session)
        {
           await CreateTableAsync(session, CREATE_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE, INDEX_BY_EVENT_TYPE_TABLE_NAME).ConfigureAwait(false);
           await CreateTableAsync(session, MessageCounter.CreateTableTemplate, "EventCounter").ConfigureAwait(false);
        }

        private async Task<IStatement> GetCreateKeySpaceQuery(ISession session)
        {
            string keyspace = cassandraProvider.GetKeyspace();
            string createKeySpaceQueryTemplate = replicationStrategy.CreateKeySpaceTemplate(keyspace);
            PreparedStatement createEventsTableStatement = await session.PrepareAsync(createKeySpaceQueryTemplate).ConfigureAwait(false);
            createEventsTableStatement.SetConsistencyLevel(ConsistencyLevel.All);

            return createEventsTableStatement.Bind();
        }

        private async Task CreateTableAsync(ISession session, string cqlQueryTemplate, string tableName)
        {
            long t0 = Stopwatch.GetTimestamp();

            string keyspace = cassandraProvider.GetKeyspace();

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("[EventStore] Creating table `{tableName}` with `{address}` in keyspace `{keyspace}`...", tableName, session.Cluster.AllHosts().First().Address, keyspace);

            string query = string.Format(cqlQueryTemplate, keyspace, tableName).ToLower();
            PreparedStatement createEventsTableStatement = await session.PrepareAsync(query).ConfigureAwait(false);
            createEventsTableStatement.SetConsistencyLevel(ConsistencyLevel.All);

            var rs = await session.ExecuteAsync(createEventsTableStatement.Bind()).ConfigureAwait(false);

            TimeSpan elapsed = Stopwatch.GetElapsedTime(t0);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("[EventStore] Created table `{tableName}` in keyspace `{keyspace}`...", tableName, keyspace);

            logger.LogInformation("[EventStore] Created table `{tableName}`... Maybe?! Is schema in agreement = {isSchemaInAgreement}. Time elapsed : {timeForExecution}", tableName, rs?.Info?.IsSchemaInAgreement, elapsed);
        }
    }
}
