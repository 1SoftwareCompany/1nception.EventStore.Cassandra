using One.Inception.AtomicAction;
using One.Inception.MessageProcessing;
using One.Inception.Multitenancy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;

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

            tenantsOptions.OnChange(OptionsChangedBootstrapEventStoreForTenant);
        }

        public void Bootstrap()
        {
            BootstrapTenants(tenants.Tenants);
        }

        private void BootstrapTenants(IEnumerable<string> tenants)
        {
            string lockKey = $"{bc.Name}{Enum.GetName(typeof(Bootstraps), Bootstraps.ExternalResource)}";
            if (@lock.LockAsync(lockKey, lockTtl).GetAwaiter().GetResult())
            {
                foreach (var tenant in tenants)
                {
                    DefaultContextFactory contextFactory = serviceProvider.GetRequiredService<DefaultContextFactory>();
                    InceptionContext context = contextFactory.Create(tenant, serviceProvider);

                    serviceProvider.GetRequiredService<CassandraEventStoreSchema>().CreateStorageAsync().GetAwaiter().GetResult();
                }

                @lock.UnlockAsync(lockKey).GetAwaiter().GetResult();
            }
            else
            {
                logger.LogWarning("[EventStore] Could not acquire lock for `{boundedContext}` to create table.", bc.Name);
            }
        }

        private void OptionsChangedBootstrapEventStoreForTenant(TenantsOptions newOptions)
        {
            if (tenants.Tenants.SequenceEqual(newOptions.Tenants) == false) // Check for difference between tenants and newOptions
            {
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("tenant options re-loaded with {@options}", newOptions);

                // Find the difference between the old and new tenants
                // and bootstrap the new tenants
                var newTenants = newOptions.Tenants.Except(tenants.Tenants);
                BootstrapTenants(newTenants);

                tenants = newOptions;
            }
        }
    }
}
