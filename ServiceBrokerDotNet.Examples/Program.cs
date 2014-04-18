using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServiceBrokerDotNet.Examples
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var sqlConnection = new SqlConnection("connectionString"))
            {
                sqlConnection.Open();

                // Create the Service Broker Service and Queue if they don't exist.
                using (var sqlTransaction = sqlConnection.BeginTransaction())
                {
                    ServiceBrokerWrapper.CreateServiceAndQueue(sqlTransaction, @"[\\Example\Service]", "ExampleServiceQueue");
                    sqlTransaction.Commit();
                }

                // Send a single message to a Service endpoint and immediately
                // end this side of the conversation
                using (var sqlTransaction = sqlConnection.BeginTransaction())
                {
                    var messageData = new byte[1000];
                    var conversationHandle =
                        ServiceBrokerWrapper.SendOne(
                        sqlTransaction
                        , @"[\\Example\Service2]"
                        , @"[\\Example\Service]"
                        , "MessageContractName"
                        , "MessageType"
                        , messageData);
                    sqlTransaction.Commit();
                }

                // Wait for a message to be available on the queue. We will
                // wait for some number of milliseconds. If the timeout expires
                // then the method returns "null". Otherwise it will contain
                // the message received from the queue.
                using (var sqlTransaction = sqlConnection.BeginTransaction())
                {
                    Message message = null;
                    while (message == null)
                    {
                        message = ServiceBrokerWrapper.WaitAndReceive(sqlTransaction, "ExampleServiceQueue", 60 * 60 * 1000);
                    }
                    // ...handle message...

                    // If we rollback the transaction then the message will
                    // return to the queue to be handled again.
                    // If we commit the transaction then we're done.
                    sqlTransaction.Commit();
                }
            }
        }
    }
}
