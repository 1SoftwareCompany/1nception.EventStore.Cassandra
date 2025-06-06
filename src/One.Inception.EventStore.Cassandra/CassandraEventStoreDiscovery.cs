﻿using System.Collections.Generic;
using System.Linq;
using One.Inception.DangerZone;
using One.Inception.Discoveries;
using One.Inception.EventStore.Index;
using One.Inception.EventStore.Cassandra.Counters;
using One.Inception.EventStore.Cassandra.Migrations;
using One.Inception.EventStore.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.DependencyInjection;

namespace One.Inception.EventStore.Cassandra
{
    public class CassandraEventStoreDiscovery : EventStoreDiscovery
    {
        protected override DiscoveryResult<IEventStore> DiscoverFromAssemblies(DiscoveryContext context)
        {
            IEnumerable<DiscoveredModel> models = base.DiscoverFromAssemblies(context).Models
                .Concat(GetModels(context))
                .Concat(DiscoverCassandraTableNameStrategy(context));

            return new DiscoveryResult<IEventStore>(models, services =>
            {
                services.AddOptions<CassandraProviderOptions, CassandraProviderOptionsProvider>();
                services.AddOptions<MigrationCassandraProviderOptions, MigrationCassandraProviderOptionsProvider>();
            });
        }

        protected virtual IEnumerable<DiscoveredModel> DiscoverCassandraTableNameStrategy(DiscoveryContext context)
        {
            yield return new DiscoveredModel(typeof(ITableNamingStrategy), typeof(TablePerBoundedContext), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(TablePerBoundedContext), typeof(TablePerBoundedContext), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(NoTableNamingStrategy), typeof(NoTableNamingStrategy), ServiceLifetime.Singleton);
        }

        IEnumerable<DiscoveredModel> GetModels(DiscoveryContext context)
        {
            yield return new DiscoveredModel(typeof(CassandraEventStore), typeof(CassandraEventStore), ServiceLifetime.Singleton) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(IEventStorePlayer), typeof(CassandraEventStore), ServiceLifetime.Singleton);

            yield return new DiscoveredModel(typeof(CassandraEventStore), typeof(CassandraEventStore), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(IEventStore), typeof(CassandraEventStore), ServiceLifetime.Singleton);

            var cassandraSettings = context.FindService<ICassandraEventStoreSettings>();
            foreach (var setting in cassandraSettings)
            {
                yield return new DiscoveredModel(setting, setting, ServiceLifetime.Transient);
            }

            yield return new DiscoveredModel(typeof(EventToAggregateRootId), typeof(EventToAggregateRootId), ServiceLifetime.Singleton) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(IEventStoreIndex), typeof(EventToAggregateRootId), ServiceLifetime.Singleton) { CanOverrideDefaults = true };

            yield return new DiscoveredModel(typeof(CassandraProvider), typeof(CassandraProvider), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(ICassandraProvider), typeof(CassandraProvider), ServiceLifetime.Singleton);

            yield return new DiscoveredModel(typeof(IKeyspaceNamingStrategy), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(NoKeyspaceNamingStrategy), typeof(NoKeyspaceNamingStrategy), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(KeyspacePerTenantKeyspace), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Singleton);

            yield return new DiscoveredModel(typeof(CassandraReplicationStrategyFactory), typeof(CassandraReplicationStrategyFactory), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(ICassandraReplicationStrategy), provider => provider.GetRequiredService<CassandraReplicationStrategyFactory>().GetReplicationStrategy(), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(IndexByEventTypeStore), typeof(IndexByEventTypeStore), ServiceLifetime.Singleton) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(IIndexStore), typeof(IndexByEventTypeStore), ServiceLifetime.Singleton) { CanOverrideDefaults = true };

            yield return new DiscoveredModel(typeof(EventStoreDataWiper), typeof(EventStoreDataWiper), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(IDangerZone), typeof(EventStoreDataWiper), ServiceLifetime.Singleton) { CanAddMultiple = true };

            yield return new DiscoveredModel(typeof(CassandraEventStoreSchema), typeof(CassandraEventStoreSchema), ServiceLifetime.Singleton);

            yield return new DiscoveredModel(typeof(MessageCounter), typeof(MessageCounter), ServiceLifetime.Singleton) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(IMessageCounter), typeof(MessageCounter), ServiceLifetime.Singleton) { CanOverrideDefaults = true };
        }
    }
}
