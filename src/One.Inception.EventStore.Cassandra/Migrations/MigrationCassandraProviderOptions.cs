using Microsoft.Extensions.Configuration;

namespace One.Inception.EventStore.Cassandra.Migrations
{
    public class MigrationCassandraProviderOptions : CassandraProviderOptions { }

    public class MigrationCassandraProviderOptionsProvider : InceptionOptionsProviderBase<MigrationCassandraProviderOptions>
    {
        public const string SettingKey = "inception:migration:source:cassandra";

        public MigrationCassandraProviderOptionsProvider(IConfiguration configuration) : base(configuration) { }

        public override void Configure(MigrationCassandraProviderOptions options)
        {
            configuration.GetSection(SettingKey).Bind(options);
        }
    }
}
