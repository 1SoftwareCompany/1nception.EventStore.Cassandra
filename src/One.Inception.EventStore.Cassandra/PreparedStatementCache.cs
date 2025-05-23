﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using One.Inception.MessageProcessing;

namespace One.Inception.EventStore.Cassandra
{
    internal abstract class PreparedStatementCache
    {
        private readonly IInceptionContextAccessor context;
        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;
        private SemaphoreSlim threadGate = new SemaphoreSlim(1);
        private Dictionary<string, PreparedStatement> _tenantCache;

        protected PreparedStatementCache(IInceptionContextAccessor context, ICassandraProvider cassandraProvider) : this(context, cassandraProvider, default) { }

        public PreparedStatementCache(IInceptionContextAccessor contextAccessor, ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy)
        {
            _tenantCache = new Dictionary<string, PreparedStatement>();

            this.context = contextAccessor ?? throw new ArgumentNullException(nameof(contextAccessor));
            this.cassandraProvider = cassandraProvider ?? throw new ArgumentNullException(nameof(cassandraProvider));
            this.tableNameStrategy = tableNameStrategy; // allows null/default
        }

        internal abstract string GetQueryTemplate();
        internal virtual string GetTableName() => tableNameStrategy?.GetName();

        internal async Task<PreparedStatement> PrepareAsync(ISession session)
        {
            try
            {
                PreparedStatement preparedStatement = default;
                if (_tenantCache.TryGetValue(context.Context.Tenant, out preparedStatement) == false)
                {
                    await threadGate.WaitAsync(10000).ConfigureAwait(false);
                    if (_tenantCache.TryGetValue(context.Context.Tenant, out preparedStatement))
                        return preparedStatement;

                    string keyspace = cassandraProvider.GetKeyspace();
                    string tableName = GetTableName();
                    string template = GetQueryTemplate();

                    if (string.IsNullOrEmpty(keyspace)) throw new Exception($"Invalid keyspace while preparing query template: {template}");
                    if (tableNameStrategy is not null && string.IsNullOrEmpty(tableName)) throw new Exception($"Invalid table name while preparing query template: {template}");

                    string query = string.Format(template, keyspace, tableName);

                    preparedStatement = await session.PrepareAsync(query).ConfigureAwait(false);
                    SetPreparedStatementOptions(preparedStatement);

                    _tenantCache.TryAdd(context.Context.Tenant, preparedStatement);
                }

                return preparedStatement;
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to prepare query statement for {this.GetType().Name}", ex);
            }
            finally
            {
                threadGate?.Release();
            }
        }

        internal virtual void SetPreparedStatementOptions(PreparedStatement statement)
        {
            statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        }
    }
}
