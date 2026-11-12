using One.Inception.AtomicAction;
using One.Inception.MessageProcessing;
using One.Inception.Multitenancy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace One.Inception.EventStore.Cassandra
{
    /// <summary>
    /// Creates the storage of the tenants. It runs once for the whole cluster, on a single node for each tenant.
    /// </summary>
    [InceptionStartup(Bootstraps.ExternalResource, ClusterWide = true)]
    public class EventStoreStartup : IInceptionStartup
    {
        internal const int MaxAttempts = 5;

        private readonly ILogger<EventStoreStartup> logger;
        private readonly IServiceProvider serviceProvider;
        private readonly ILock @lock;
        private readonly SemaphoreSlim nodeLock = new SemaphoreSlim(1, 1);
        private readonly Func<int, TimeSpan> attemptDelay;
        private TenantsOptions tenants;
        private BoundedContext bc;
        private readonly TimeSpan lockTtl;

        public EventStoreStartup(IOptionsMonitor<BoundedContext> bc, IServiceProvider serviceProvider, IOptionsMonitor<TenantsOptions> tenantsOptions, ILock @lock, ILogger<EventStoreStartup> logger)
            : this(bc, serviceProvider, tenantsOptions, @lock, logger, attempt => TimeSpan.FromSeconds(attempt)) { }

        internal EventStoreStartup(IOptionsMonitor<BoundedContext> bc, IServiceProvider serviceProvider, IOptionsMonitor<TenantsOptions> tenantsOptions, ILock @lock, ILogger<EventStoreStartup> logger, Func<int, TimeSpan> attemptDelay)
        {
            this.serviceProvider = serviceProvider;
            this.tenants = tenantsOptions.CurrentValue;
            this.logger = logger;
            this.bc = bc.CurrentValue; // We decide that changing the bounded context is not supported, because if we change it runtime we could have a lot of problems.
            this.@lock = @lock;
            this.attemptDelay = attemptDelay;

            this.lockTtl = TimeSpan.FromSeconds(2);
            if (lockTtl == TimeSpan.Zero) throw new ArgumentException("Lock ttl must be more than 0", nameof(lockTtl));

            tenantsOptions.OnChange(TenantOptionsChanged);
        }

        public Task BootstrapAsync()
        {
            return BootstrapTenantsInternalAsync(tenants.Tenants);
        }

        public Task BootstrapAsync(IEnumerable<string> tenants)
        {
            return BootstrapTenantsInternalAsync(tenants);
        }

        /// <summary>
        /// A node bootstraps its tenants in parallel, so they take turns on the node before they take the lock of the bounded context,
        /// which another node may hold while it creates the storage of another tenant.
        /// </summary>
        private async Task BootstrapTenantsInternalAsync(IEnumerable<string> tenants)
        {
            await nodeLock.WaitAsync().ConfigureAwait(false);
            try
            {
                await CreateStorageInClusterLockAsync(tenants.ToList()).ConfigureAwait(false);
            }
            finally
            {
                nodeLock.Release();
            }
        }

        private async Task CreateStorageInClusterLockAsync(List<string> tenants)
        {
            string lockKey = $"{bc.Name}{Enum.GetName(typeof(Bootstraps), Bootstraps.ExternalResource)}";

            for (int attempt = 1; attempt <= MaxAttempts; attempt++)
            {
                if (await @lock.LockAsync(lockKey, lockTtl).ConfigureAwait(false))
                {
                    try
                    {
                        foreach (var tenant in tenants)
                        {
                            await CreateStorageAsync(tenant).ConfigureAwait(false);
                        }
                        return;
                    }
                    finally
                    {
                        await @lock.UnlockAsync(lockKey).ConfigureAwait(false);
                    }
                }
                else
                {
                    logger.LogWarning("[EventStore] Could not acquire lock for `{boundedContext}` to create table. Attempt {attempt}/{maxAttempts}.", bc.Name, attempt, MaxAttempts);
                }

                if (attempt < MaxAttempts)
                    await Task.Delay(attemptDelay(attempt)).ConfigureAwait(false);
            }

            // Only this node creates the storage of these tenants, so it does not skip it. The storage is created only if it does not exist, so creating it again is safe.
            // Failing instead would restart the node while the lock is not available, for example while Redis is down.
            logger.LogWarning("[EventStore] Failed to acquire lock for `{boundedContext}` after {maxAttempts} attempts. Creating the storage of tenants {@tenants} without the lock.", bc.Name, MaxAttempts, tenants);
            foreach (var tenant in tenants)
            {
                await CreateStorageAsync(tenant).ConfigureAwait(false);
            }
        }

        internal virtual async Task CreateStorageAsync(string tenant)
        {
            DefaultContextFactory contextFactory = serviceProvider.GetRequiredService<DefaultContextFactory>();
            InceptionContext context = contextFactory.Create(tenant, serviceProvider);

            await serviceProvider.GetRequiredService<CassandraEventStoreSchema>().CreateStorageAsync().ConfigureAwait(false);
        }

        private void TenantOptionsChanged(TenantsOptions newOptions)
        {
            if (tenants.Tenants.SequenceEqual(newOptions.Tenants) == false) // Check for difference between tenants and newOptions
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("tenant options re-loaded with {@options}", newOptions);

                tenants = newOptions;
            }
        }
    }
}
