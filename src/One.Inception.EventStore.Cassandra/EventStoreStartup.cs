using One.Inception.AtomicAction;
using One.Inception.MessageProcessing;
using One.Inception.Multitenancy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace One.Inception.EventStore.Cassandra
{
    [InceptionStartup(Bootstraps.ExternalResource)]
    public class EventStoreStartup : IInceptionStartup
    {
        private readonly ILogger<EventStoreStartup> logger;
        private readonly IServiceProvider serviceProvider;
        private readonly ILock @lock;
        private TenantsOptions tenants;
        private BoundedContext bc;
        private readonly TimeSpan lockTtl;

        public EventStoreStartup(IOptionsMonitor<BoundedContext> bc, IServiceProvider serviceProvider, IOptionsMonitor<TenantsOptions> tenantsOptions, ILock @lock, ILogger<EventStoreStartup> logger)
        {
            this.serviceProvider = serviceProvider;
            this.tenants = tenantsOptions.CurrentValue;
            this.logger = logger;
            this.bc = bc.CurrentValue; // We decide that changing the bounded context is not supported, because if we change it runtime we could have a lot of problems.
            this.@lock = @lock;

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

        private async Task BootstrapTenantsInternalAsync(IEnumerable<string> tenants)
        {
            const int maxAttempts = 5;
            string lockKey = $"{bc.Name}{Enum.GetName(typeof(Bootstraps), Bootstraps.ExternalResource)}";

            for (int attempt = 1; attempt <= maxAttempts; attempt++)
            {
                if (await @lock.LockAsync(lockKey, lockTtl).ConfigureAwait(false))
                {
                    try
                    {
                        foreach (var tenant in tenants)
                        {
                            DefaultContextFactory contextFactory = serviceProvider.GetRequiredService<DefaultContextFactory>();
                            InceptionContext context = contextFactory.Create(tenant, serviceProvider);

                            await serviceProvider.GetRequiredService<CassandraEventStoreSchema>().CreateStorageAsync().ConfigureAwait(false);
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
                    logger.LogWarning("[EventStore] Could not acquire lock for `{boundedContext}` to create table. Attempt {attempt}/{maxAttempts}.", bc.Name, attempt, maxAttempts);
                }

                if (attempt < maxAttempts)
                    await Task.Delay(TimeSpan.FromSeconds(attempt)).ConfigureAwait(false);
            }

            logger.LogError("[EventStore] Failed to acquire lock for `{boundedContext}` after {maxAttempts} attempts.", bc.Name, maxAttempts);
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
