using System;
using System.Threading.Tasks;
using Cassandra;
using One.Inception.DangerZone;
using One.Inception.MessageProcessing;
using Microsoft.Extensions.Logging;
using System.Threading;

namespace One.Inception.EventStore.Cassandra;

public class EventStoreDataWiper : IDangerZone
{
    private readonly ICassandraProvider cassandraProvider;
    private readonly IInceptionContextAccessor contextAccessor;
    private readonly ILogger<EventStoreDataWiper> logger;

    private DropKeyspaceQuery _dropKeyspaceQuery;

    private Task<ISession> GetSessionAsync(CancellationToken cancellationToken) => cassandraProvider.GetWipeSessionAsync(cancellationToken); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

    public EventStoreDataWiper(IInceptionContextAccessor contextAccessor, ICassandraProvider cassandraProvider, ILogger<EventStoreDataWiper> logger)
    {
        if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

        this.cassandraProvider = cassandraProvider;
        this.contextAccessor = contextAccessor;
        this.logger = logger;

        _dropKeyspaceQuery = new DropKeyspaceQuery(contextAccessor, cassandraProvider);
    }

    public async Task WipeDataAsync(string tenant)
    {
        CancellationToken cancellationToken = contextAccessor.Context.CancellationToken;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (tenant.Equals(contextAccessor.Context.Tenant, StringComparison.Ordinal) == false)
            {
                logger.LogError("Tenant mismatch. The tenant to be wiped is different from the current tenant.");
                return;
            }

            ISession session = await GetSessionAsync(cancellationToken).ConfigureAwait(false);
            PreparedStatement statement = await _dropKeyspaceQuery.PrepareAsync(session, cancellationToken).ConfigureAwait(false);

            logger.LogInformation("Wiping eventstore data for tenant {tenant} query {query}", tenant, statement.QueryString);

            cancellationToken.ThrowIfCancellationRequested(); // last chance, the driver cannot cancel the drop once it is sent

            var bs = statement.Bind().SetIdempotence(true);
            await session.ExecuteAsync(bs).ConfigureAwait(false);

            logger.LogInformation("Eventstore data for tenant {tenant} wiped", tenant);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to drop keyspace.");
            throw;
        }
    }

    class DropKeyspaceQuery : PreparedStatementCache
    {
        private const string Template = @"DROP KEYSPACE IF EXISTS {0};";

        public DropKeyspaceQuery(IInceptionContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider) { }

        internal override string GetQueryTemplate() => Template;
    }
}
