using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace ServiceBrokerDotNet
{
    public static class ServiceBusWatcherExtensions
    {
        public static IEnumerable<string> GetQueueNames(this IDbConnection connection)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = "SELECT name FROM sys.service_queues ORDER BY name";
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        yield return reader.GetString(0);
                    }
                }
            }
        }

        /// <summary>
        /// Returns the estimated size of each queue. The results are NOT buffered (yield return from the internal reader).
        /// </summary>
        public static IEnumerable<QueueCount> GetEstimatedRowCountInQueue(this IDbConnection connection, params string[] queues)
        {
            if (!queues.Any())
            {
                throw new ArgumentException("Need at least 1 queue to query");
            }
            using (var cmd = connection.CreateCommand())
            {
                var paramString = "";
                for (int i = 0; i < queues.Length; i++)
                {
                    var param = cmd.CreateParameter();
                    param.ParameterName = "@p" + i;
                    param.Value = queues[i].Trim();
                    param.DbType = DbType.String;
                    cmd.Parameters.Add(param);
                    paramString += param.ParameterName;
                    if (i != queues.Length - 1)
                    {
                        paramString += ",";
                    }
                }

                cmd.CommandText = string.Format(Query_QueuePartition, paramString);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        yield return new QueueCount() { Name = reader.GetString(0), Count = (int)reader.GetInt64(1) };
                    }
                }
            }
        }

        /// <summary>
        /// Returns the current number of poison messages from each queue. The results are NOT buffered (yield return from the internal reader).
        /// </summary>
        public static IEnumerable<QueueCount> GetPoisonMessageCountForQueue(this IDbConnection connection, params string[] queues)
        {
            if (!queues.Any())
            {
                throw new ArgumentException("Need at least 1 queue to query");
            }
            using (var cmd = connection.CreateCommand())
            {
                var paramString = "";
                for (int i = 0; i < queues.Length; i++)
                {
                    var param = cmd.CreateParameter();
                    param.ParameterName = "@p" + i;
                    param.Value = queues[i].Trim();
                    param.DbType = DbType.String;
                    cmd.Parameters.Add(param);
                    paramString += param.ParameterName;
                    if (i != queues.Length - 1)
                    {
                        paramString += ",";
                    }
                }

                cmd.CommandText = string.Format("SELECT QueueName, COUNT(*) FROM FailedMessage WITH (NOLOCK) WHERE QueueName IN ({0}) GROUP BY QueueName", paramString);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        yield return new QueueCount() { Name = reader.GetString(0), Count = reader.GetInt32(1) };
                    }
                }
            }
        }

        /// <summary>
        /// Returns the info for poison messages found in a queue. The results are NOT buffered (yield return from the internal reader).
        /// </summary>
        public static IEnumerable<PoisonMessageInfo> GetPoisonMessages(this IDbConnection connection, string queue, int page, int maxResults, SortOrder sortOrder = SortOrder.Descending)
        {
            using (var cmd = connection.CreateCommand())
            {
                var first = (page * maxResults) - maxResults + 1;
                var last = first + maxResults - 1;

                var paramQueue = cmd.CreateParameter();
                paramQueue.ParameterName = "@queue";
                paramQueue.Value = queue;
                paramQueue.DbType = DbType.String;
                cmd.Parameters.Add(paramQueue);

                var paramFirst = cmd.CreateParameter();
                paramFirst.ParameterName = "@first";
                paramFirst.Value = first;
                paramFirst.DbType = DbType.Int32;
                cmd.Parameters.Add(paramFirst);

                var paramLast = cmd.CreateParameter();
                paramLast.ParameterName = "@last";
                paramLast.Value = last;
                paramLast.DbType = DbType.Int32;
                cmd.Parameters.Add(paramLast);

                cmd.CommandText = string.Format(Query_PosionMessages, sortOrder == SortOrder.Ascending ? "ASC" : "DESC");
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        yield return new PoisonMessageInfo()
                        {
                            MessageId = reader.GetGuid(0),
                            InsertDateTime = reader.GetDateTime(1),
                            Queue = reader.GetString(2),
                            QueueService = reader.GetString(3),
                            OriginService = reader.GetString(4),
                        };
                    }
                }
            }
        }

        /// <summary>
        /// Gets a single Poison Message, including message body and exception details.
        /// </summary>
        public static PoisonMessage GetPoisonMessage(this IDbConnection connection, Guid messageId)
        {
            using (var cmd = connection.CreateCommand())
            {
                var param = cmd.CreateParameter();
                param.ParameterName = "@messageId";
                param.Value = messageId;
                param.DbType = DbType.Guid;
                cmd.Parameters.Add(param);
                cmd.CommandText = Query_PosionMessage;
                using (var reader = (SqlDataReader)cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var message = new PoisonMessage()
                        {
                            Info = new PoisonMessageInfo()
                            {
                                MessageId = reader.GetGuid(0),
                                InsertDateTime = reader.GetDateTime(1),
                                Queue = reader.GetString(2),
                                QueueService = reader.GetString(3),
                                OriginService = reader.GetString(4),
                            },
                            Message = reader.GetString(5) ?? string.Empty,
                            ExceptionMessage = reader.GetString(6) ?? string.Empty,
                        };
                        return message;
                    }
                    return null;
                }
            }
        }

        /// <summary>
        /// Resends a single message to the queue from which it failed.
        /// </summary>
        public static void ResendPoisonMessage(this IDbConnection connection, Guid messageId, string originService = null)
        {
            if (string.IsNullOrWhiteSpace(originService))
            {
                var poisonMessage = GetPoisonMessage(connection, messageId);
                if (poisonMessage == null)
                {
                    return;
                }
                originService = poisonMessage.Info.OriginService;
            }
            using (var cmd = connection.CreateCommand())
            {
                var param = cmd.CreateParameter();
                param.ParameterName = "@messageId";
                param.Value = messageId;
                param.DbType = DbType.Guid;
                cmd.Parameters.Add(param);
                cmd.CommandText = string.Format(Resend_Single_Message, originService);
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Resends poison messages found in a queue
        /// </summary>
        public static void ResendAllPoisonMessages(this IDbConnection connection, string queue)
        {
            var count = GetPoisonMessageCountForQueue(connection, queue).First().Count;
            while (count > 0)
            {
                var top100 = GetPoisonMessages(connection, queue, 1, 100, SortOrder.Ascending).ToList(); // ToList() to buffer into memory and close the reader
                foreach (var item in top100)
                {
                    if (string.IsNullOrWhiteSpace(item.OriginService))
                    {
                        // Cannot resend messages without origin service
                        continue;
                    }
                    ResendPoisonMessage(connection, item.MessageId, item.OriginService);
                }
                count = count - 100;
            }
        }

        /// <summary>
        /// Purges (deletes) a single message referenced by its MessageId
        /// </summary>
        public static int PurgePoisonMessage(this IDbConnection connection, Guid messageId)
        {
            using (var cmd = connection.CreateCommand())
            {
                var param = cmd.CreateParameter();
                param.ParameterName = "@messageId";
                param.Value = messageId;
                param.DbType = DbType.Guid;
                cmd.Parameters.Add(param);
                cmd.CommandText = "DELETE FROM FailedMessage WHERE MessageId = @messageId";
                return cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Purges (deletes) ALL messages for a given queue. USE WITH CAUTION!
        /// </summary>
        public static int PurgeAllPoisonMessage(this IDbConnection connection, string queue)
        {
            using (var cmd = connection.CreateCommand())
            {
                var param = cmd.CreateParameter();
                param.ParameterName = "@queue";
                param.Value = queue;
                param.DbType = DbType.String;
                cmd.Parameters.Add(param);
                cmd.CommandText = "DELETE FROM FailedMessage WHERE QueueName = @queue";
                return cmd.ExecuteNonQuery();
            }
        }

        private static readonly string Query_QueuePartition = @"
SELECT q.name, p.rows
FROM sys.partitions p
	INNER JOIN sys.internal_tables t ON t.object_id = p.object_id
	INNER JOIN sys.service_queues q ON q.object_id = t.parent_object_id
WHERE p.index_id IN (1, 0) AND q.name IN ({0});
";

        private static readonly string Query_PosionMessages = @"
SELECT * FROM
 (SELECT 
	MessageId,
	InsertDateTime,
	QueueName,
	QueueService,
	OriginService,
	ROW_NUMBER() OVER (ORDER BY InsertDateTime {0}) as RowNumber
	FROM FailedMessage WITH (NOLOCK)
    WHERE QueueName = @queue) _
WHERE RowNumber BETWEEN @first AND @last;
";

        private static readonly string Query_PosionMessage = @"
SELECT 
	MessageId,
	InsertDateTime,
	QueueName,
	QueueService,
	OriginService,
	CAST(CAST(MessageData AS VARBINARY(MAX)) AS VARCHAR(MAX)) AS MessageData,
	ErrorMessage
FROM FailedMessage WITH (NOLOCK)
WHERE MessageId = @messageId;
";

        private static readonly string Resend_Single_Message = @"
            DECLARE @ch uniqueidentifier
	        DECLARE @destService nvarchar(255)
	        DECLARE @body nvarchar(max)

	        SELECT 
		        TOP 1 
		          @destService = QueueService
		        , @body = MessageData 
	        FROM FailedMessage
	        WHERE MessageId = @messageId

            SET @destService = REPLACE(REPLACE(@destService, '[', ''), ']', '')

	        IF (@body IS NOT NULL)
	        BEGIN
		        BEGIN TRAN
		        BEGIN DIALOG @ch
			        FROM SERVICE {0}
			        TO SERVICE @destService
			        ON CONTRACT NServiceBusTransportMessageContract
                    WITH ENCRYPTION = OFF;
            

		        SEND
		           ON CONVERSATION @ch
		           MESSAGE TYPE NServiceBusTransportMessage
		           (@body)
		   
		        END CONVERSATION @ch

		        DELETE FROM FailedMessage WHERE MessageId = @messageId
		        COMMIT TRAN
	        END
        ";
    }
}
