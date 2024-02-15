﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elders.Cronus.EventStore;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Migrations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra.Migrations
{
    public sealed class MigrateEventStore
    {
        private readonly IServiceProvider _serviceProvider;
        private IMigrationEventStorePlayer _sourcePlayer;
        private ISerializer _serializer;
        private ICronusMigratorManual _migrator;
        private readonly ILogger<MigrateEventStore> logger;

        public MigrateEventStore(IServiceProvider serviceProvider, ILogger<MigrateEventStore> logger)
        {
            _serviceProvider = serviceProvider;
            this.logger = logger;
        }

        public async Task RunMigratorAsync(string tenant, PlayerOptions playerOptions, CancellationToken cancellationToken = default)
        {
            using (IServiceScope scope = _serviceProvider.CreateScope())
            {
                InitializeTenantContext(scope.ServiceProvider, tenant);

                _sourcePlayer = scope.ServiceProvider.GetRequiredService<IMigrationEventStorePlayer>();
                _migrator = scope.ServiceProvider.GetRequiredService<ICronusMigratorManual>();
                _serializer = scope.ServiceProvider.GetRequiredService<ISerializer>();

                await RunAsync(tenant, playerOptions, cancellationToken).ConfigureAwait(false);
            }
        }

        private void InitializeTenantContext(IServiceProvider serviceProvider, string tenant)
        {
            DefaultCronusContextFactory cronusContextFactory = serviceProvider.GetRequiredService<DefaultCronusContextFactory>();
            CronusContext cronusContext = cronusContextFactory.Create(tenant, serviceProvider);
        }

        private static uint MigratedAggregates = 0;
        private async Task RunAsync(string tenant, PlayerOptions playerOptions, CancellationToken cancellationToken)
        {
            MigratedAggregates = 0;
            var @operator = new PlayerOperator()
            {
                OnAggregateStreamLoadedAsync = async arStream =>
                {
                    if (MigratedAggregates % 1000 == 0)
                        logger.LogInformation("Migration from v9 to v10 migrated {count} aggregates for {cronus_tenant}.", MigratedAggregates, tenant);

                    foreach (AggregateCommitRaw commitRaw in arStream.Commits)
                    {
                        List<IEvent> @events = new List<IEvent>();
                        List<IPublicEvent> publicEvents = new List<IPublicEvent>();

                        var messages = commitRaw.Events.Select(@event => _serializer.DeserializeFromBytes<IMessage>(@event.Data));
                        foreach (IMessage msg in messages)
                        {
                            if (msg is IEvent @event)
                                @events.Add(@event);
                            else if (msg is IPublicEvent publicEvent)
                                publicEvents.Add(publicEvent);
                        }

                        var firstEvent = commitRaw.Events.First();
                        var id = firstEvent.AggregateRootId;
                        var rev = firstEvent.Revision;
                        var ts = firstEvent.Timestamp;
                        var sourceCommit = new AggregateCommit(id, rev, @events, publicEvents, ts);

                        await _migrator.MigrateAsync(sourceCommit).ConfigureAwait(false);
                        MigratedAggregates++;
                    }
                }
            };

            logger.LogInformation("Migration from v9 to v10 has started for tenant {cronus_tenant}...", tenant);
            await _sourcePlayer.EnumerateEventStore(@operator, playerOptions, cancellationToken).ConfigureAwait(false);
            logger.LogInformation("Migration from v9 to v10 has finished for tenant {cronus_tenant}!", tenant);
        }
    }
}
