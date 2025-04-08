using Cassandra;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace One.Inception.EventStore.Cassandra.MessageStore
{
    public interface IMessageStore
    {
        Task AppendAsync(InceptionMessage msg);
        IAsyncEnumerable<InceptionMessage> LoadMessagesAsync(int batchSize);
    }

    public class CassandraMessageStore : IMessageStore
    {
        private static readonly ILogger logger = InceptionLogger.CreateLogger(typeof(CassandraMessageStore));

        private const string MESSAGE_STORE_TABLE_NAME = "Message_Store";
        private const string INSERT_MESSAGE_QUERY_TEMPLATE = @"INSERT INTO ""{0}"" (date,ts,data) VALUES (?,?,?);";
        private const string LOAD_MESSAGES_QUERY_TEMPLATE = @"SELECT data FROM {0};";

        private readonly ISerializer serializer;
        private readonly ISession session;

        public CassandraMessageStore(ISession session, ISerializer serializer)
        {
            this.session = session;
            this.serializer = serializer;
        }

        public async Task AppendAsync(InceptionMessage msg)
        {
            var date = DateTime.UtcNow;
            var cutDownDate = Convert.ToDateTime(date.ToString("yyyyMMdd"));
            var dateTimeStamp = cutDownDate.ToFileTimeUtc();


            long resultTimestamp = DateTime.UtcNow.ToFileTimeUtc();

            string publishTime;
            if (msg.Headers.TryGetValue(MessageHeader.PublishTimestamp, out publishTime))
                if (long.TryParse(publishTime, out resultTimestamp)) { }

            byte[] data = serializer.SerializeToBytes(msg);

            PreparedStatement insertPreparedStatement = await session.PrepareAsync(string.Format(INSERT_MESSAGE_QUERY_TEMPLATE, MESSAGE_STORE_TABLE_NAME)).ConfigureAwait(false);

            await session
                .ExecuteAsync(insertPreparedStatement
                .Bind(dateTimeStamp, resultTimestamp, data))
                .ConfigureAwait(false);
        }

        public async IAsyncEnumerable<InceptionMessage> LoadMessagesAsync(int batchSize)
        {
            PreparedStatement loadMessagesPreparedStatement = await session.PrepareAsync(string.Format(LOAD_MESSAGES_QUERY_TEMPLATE, MESSAGE_STORE_TABLE_NAME)).ConfigureAwait(false);

            var queryStatement = loadMessagesPreparedStatement.Bind().SetPageSize(batchSize);
            var result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                var data = row.GetValue<byte[]>("data");
                InceptionMessage commit = serializer.DeserializeFromBytes<InceptionMessage>(data);

                yield return commit;
            }
        }
    }
}
