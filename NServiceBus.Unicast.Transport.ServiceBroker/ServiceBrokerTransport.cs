using System;
using System.Collections.Generic;
using System.Xml.Serialization;
using log4net;
using NServiceBus.Serialization;
using NServiceBus.Utils;
using System.Threading;
using NServiceBus.Unicast.Transport.Msmq;
using ServiceBroker.Net;
using System.Transactions;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Runtime.Serialization.Formatters.Binary;

namespace NServiceBus.Unicast.Transport.ServiceBroker {
    public class ServiceBrokerTransport : ITransport {

        public ServiceBrokerTransport() {
            MaxRetries = 5;
            SecondsToWaitForMessage = 10;
        }

        #region members

        private readonly IList<WorkerThread> workerThreads = new List<WorkerThread>();

        private readonly ReaderWriterLockSlim failuresPerConversationLocker = new ReaderWriterLockSlim();
        /// <summary>
        /// Accessed by multiple threads - lock using failuresPerConversationLocker.
        /// </summary>
        private readonly IDictionary<string, int> failuresPerConversation = new Dictionary<string, int>();

        [ThreadStatic]
        private static volatile bool _needToAbort;

        [ThreadStatic]
        private static volatile string conversationHandle;

        [ThreadStatic]
        private static SqlServiceBrokerTransactionManager transactionManager;

        private static readonly ILog Logger = LogManager.GetLogger(typeof(ServiceBrokerTransport));

        private readonly XmlSerializer headerSerializer = new XmlSerializer(typeof(List<HeaderInfo>));
        #endregion

        #region config info

        /// <summary>
        /// The name of the service that is appended as the reply-to endpoint
        /// </summary>
        public string ReplyToService { get; set; }

        /// <summary>
        /// Sql connection string to the service hosting the service broker
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// The path to the queue the transport will read from.
        /// </summary>
        public string InputQueue { get; set; }

        /// <summary>
        /// Sets the service the transport will transfer errors to.
        /// </summary>
        public string ErrorService { get; set; }

        /// <summary>
        /// Sets whether or not the transport is transactional.
        /// </summary>
        public bool IsTransactional { get; set; }

        /// <summary>
        /// Sets whether a distributed transaction scope is to be used
        /// </summary>
        public bool UseDistributedTransaction { get; set; }

        /// <summary>
        /// Sets whether or not the transport should deserialize
        /// the body of the message placed on the queue.
        /// </summary>
        public bool SkipDeserialization { get; set; }

        /// <summary>
        /// Sets the maximum number of times a message will be retried
        /// when an exception is thrown as a result of handling the message.
        /// This value is only relevant when <see cref="IsTransactional"/> is true.
        /// </summary>
        /// <remarks>
        /// Default value is 5.
        /// </remarks>
        public int MaxRetries { get; set; }

        /// <summary>
        /// Sets the maximum interval of time for when a thread thinks there is a message in the queue
        /// that it tries to receive, until it gives up.
        /// 
        /// Default value is 10.
        /// </summary>
        public int SecondsToWaitForMessage { get; set; }

        /// <summary>
        /// Property for getting/setting the period of time when the transaction times out.
        /// Only relevant when <see cref="IsTransactional"/> is set to true.
        /// </summary>
        public TimeSpan TransactionTimeout { get; set; }

        /// <summary>
        /// Property for getting/setting the isolation level of the transaction scope.
        /// Only relevant when <see cref="IsTransactional"/> is set to true.
        /// </summary>
        public IsolationLevel IsolationLevel { get; set; }

        /// <summary>
        /// Sets the object which will be used to serialize and deserialize messages.
        /// </summary>
        public IMessageSerializer MessageSerializer { get; set; }

        #endregion

        #region ITransport Members

        /// <summary>
        /// Event which indicates that message processing has started.
        /// </summary>
        public event EventHandler StartedMessageProcessing;

        /// <summary>
        /// Event which indicates that message processing has completed.
        /// </summary>
        public event EventHandler FinishedMessageProcessing;

        /// <summary>
        /// Event which indicates that message processing failed for some reason.
        /// </summary>
        public event EventHandler FailedMessageProcessing;

        /// <summary>
        /// Gets/sets the number of concurrent threads that should be
        /// created for processing the queue.
        /// 
        /// Get returns the actual number of running worker threads, which may
        /// be different than the originally configured value.
        /// 
        /// When used as a setter, this value will be used by the <see cref="Start"/>
        /// method only and will have no effect if called afterwards.
        /// 
        /// To change the number of worker threads at runtime, call <see cref="ChangeNumberOfWorkerThreads"/>.
        /// </summary>
        public virtual int NumberOfWorkerThreads {
            get {
                lock (workerThreads)
                    return workerThreads.Count;
            }
            set {
                numberOfWorkerThreads = value;
            }
        }
        private int numberOfWorkerThreads;


        /// <summary>
        /// Event raised when a message has been received in the input queue.
        /// </summary>
        public event EventHandler<TransportMessageReceivedEventArgs> TransportMessageReceived;

        /// <summary>
        /// Gets the address the service
        /// </summary>
        public string Address {
            get {
                return ReplyToService;
            }
        }

        /// <summary>
        /// Changes the number of worker threads to the given target,
        /// stopping or starting worker threads as needed.
        /// </summary>
        /// <param name="targetNumberOfWorkerThreads"></param>
        public void ChangeNumberOfWorkerThreads(int targetNumberOfWorkerThreads) {
            lock (workerThreads) {
                var current = workerThreads.Count;

                if (targetNumberOfWorkerThreads == current)
                    return;

                if (targetNumberOfWorkerThreads < current) {
                    for (var i = targetNumberOfWorkerThreads; i < current; i++)
                        workerThreads[i].Stop();

                    return;
                }

                if (targetNumberOfWorkerThreads > current) {
                    for (var i = current; i < targetNumberOfWorkerThreads; i++)
                        AddWorkerThread().Start();

                    return;
                }
            }
        }

        /// <summary>
        /// Starts the transport.
        /// </summary>
        public void Start() {
            CheckConfiguration();

            if (!string.IsNullOrEmpty(InputQueue)) {
                for (int i = 0; i < numberOfWorkerThreads; i++)
                    AddWorkerThread().Start();
            }
        }

        private void CheckConfiguration() {
            if (MessageSerializer == null && !SkipDeserialization)
                throw new InvalidOperationException("No message serializer has been configured.");
        }

        /// <summary>
        /// Re-queues a message for processing at another time.
        /// </summary>
        /// <param name="m">The message to process later.</param>
        /// <remarks>
        /// This method will place the message onto the back of the queue
        /// which may break message ordering.
        /// </remarks>
        public void ReceiveMessageLater(TransportMessage m) {
            if (!string.IsNullOrEmpty(ReplyToService))
                Send(m, ReplyToService);
        }

        /// <summary>
        /// Sends a message to the specified destination.
        /// </summary>
        /// <param name="m">The message to send.</param>
        /// <param name="destination">The address of the destination to send the message to.</param>
        public void Send(TransportMessage m, string destination) {
            GetSqlTransactionManager().RunInTransaction(transaction => {
                transaction.Save("UndoReceiveOnSend");
                try {

                    // Get the known message types contained within the transport message
                    //var knownTypes = new Type[0];
                    //foreach (var item in m.Headers) {
                    //    if (item.Key == "EnclosedMessageTypes") {
                    //        var types = UnicastBus.DeserializeEnclosedMessageTypes(item.Value);
                    //        knownTypes = new Type[types.Count];
                    //        for (int i = 0; i < types.Count; i++) {
                    //            knownTypes[i] = Type.GetType(types[i]);
                    //        }
                    //    }
                    //}

                    // Always begin and end a conversation to simulate a monologe
                    var conversationHandle = ServiceBrokerWrapper.BeginConversation(transaction, ReplyToService, destination, "NServiceBusTransportMessageContract");

                    // Use the conversation handle as the message Id
                    m.Id = conversationHandle.ToString();

                    var stream = new MemoryStream(1);
                    // Serialize the transport message
                    new BinaryFormatter().Serialize(stream, m);


                    ServiceBrokerWrapper.Send(transaction, conversationHandle, "NServiceBusTransportMessage", stream.GetBuffer());
                    ServiceBrokerWrapper.EndConversation(transaction, conversationHandle);
                } catch (Exception) {
                    transaction.Rollback("UndoReceiveOnSend");
                    throw;
                }
            });
        }

        /// <summary>
        /// Causes the processing of the current message to be aborted.
        /// </summary>
        public void AbortHandlingCurrentMessage() {
            _needToAbort = true;
        }


        /// <summary>
        /// Returns the number of messages in the queue.
        /// </summary>
        /// <returns></returns>
        public int GetNumberOfPendingMessages() {
            throw new NotImplementedException();
        }


        #endregion

        private WorkerThread AddWorkerThread() {
            lock (workerThreads) {
                var result = new WorkerThread(Process);

                workerThreads.Add(result);

                result.Stopped += delegate(object sender, EventArgs e) {
                    var wt = sender as WorkerThread;
                    lock (workerThreads)
                        workerThreads.Remove(wt);
                };

                return result;
            }
        }

        private void Process() {
            _needToAbort = false;
            conversationHandle = string.Empty;

            if (IsTransactional)
                new TransactionWrapper().RunInTransaction(ProcessFromQueue, IsolationLevel, TransactionTimeout);
            else
                ProcessFromQueue();
        }

        private void ProcessFromQueue() {
            GetSqlTransactionManager().RunInTransaction(transaction => {

                // Create a transaction save point to rollback (instead of rolling back the ENTIRE transaction)
                transaction.Save("UndoReceiveOnReceive");

                try {
                    ReceiveFromQueue(transaction);
                    ClearFailuresForConversation(conversationHandle);
                } catch (AbortHandlingCurrentMessageException) {
                    // Roll back to our save point
                    transaction.Rollback("UndoReceiveOnReceive");
                    // in case AbortHandlingCurrentMessage was called
                    // don't increment failures, we want this message kept around.
                } catch {
                    // Roll back to our save point
                    transaction.Rollback("UndoReceiveOnReceive");
                    if (IsTransactional)
                        IncrementFailuresForConversation(conversationHandle);
                    OnFailedMessageProcessing();
                }
            });
        }

        private void ReceiveFromQueue(SqlTransaction transaction) {
            Message message = null;
            try {
                message = ServiceBrokerWrapper.WaitAndReceive(transaction, InputQueue, SecondsToWaitForMessage);
            } catch (Exception e) {
                Logger.Error("Error in receiving message from queue.", e);
                throw; // Throw to rollback 
            }

            // No message? That's okay
            if (message == null)
                return;

            Guid conversationHandle = message.ConversationHandle;
            ServiceBrokerTransport.conversationHandle = message.ConversationHandle.ToString();

            // Only handle transport messages
            if (message.MessageTypeName == "NServiceBusTransportMessage") {

                // exceptions here will cause a rollback - which is what we want.
                if (StartedMessageProcessing != null)
                    StartedMessageProcessing(this, null);

                TransportMessage transportMessage = null;
                try {
                    // deserialize
                    transportMessage = new BinaryFormatter().Deserialize(message.BodyStream) as TransportMessage;
                } catch (Exception e) {
                    Logger.Error("Error in deserializing message from queue.", e);
                    throw; // Throw to rollback 
                }

                // Set the correlation Id
                if (string.IsNullOrEmpty(transportMessage.IdForCorrelation))
                    transportMessage.IdForCorrelation = transportMessage.Id;

                // care about failures here
                var exceptionNotThrown = OnTransportMessageReceived(transportMessage);
                // and here
                var otherExNotThrown = OnFinishedMessageProcessing();

                // but need to abort takes precedence - failures aren't counted here,
                // so messages aren't moved to the error queue.
                if (_needToAbort)
                    throw new AbortHandlingCurrentMessageException();

                if (!(exceptionNotThrown && otherExNotThrown)) //cause rollback
                    throw new ApplicationException("Exception occured while processing message.");
            }

            // End the conversation
            ServiceBrokerWrapper.EndConversation(transaction, conversationHandle);
        }

        private bool HandledMaxRetries(string messageId) {
            failuresPerConversationLocker.EnterReadLock();

            if (failuresPerConversation.ContainsKey(messageId) &&
                   (failuresPerConversation[messageId] >= MaxRetries)) {
                failuresPerConversationLocker.ExitReadLock();
                failuresPerConversationLocker.EnterWriteLock();
                failuresPerConversation.Remove(messageId);
                failuresPerConversationLocker.ExitWriteLock();

                return true;
            }

            failuresPerConversationLocker.ExitReadLock();
            return false;
        }

        private void ClearFailuresForConversation(string conversationHandle) {
            failuresPerConversationLocker.EnterReadLock();
            if (failuresPerConversation.ContainsKey(conversationHandle)) {
                failuresPerConversationLocker.ExitReadLock();
                failuresPerConversationLocker.EnterWriteLock();
                failuresPerConversation.Remove(conversationHandle);
                failuresPerConversationLocker.ExitWriteLock();
            } else
                failuresPerConversationLocker.ExitReadLock();
        }

        private void IncrementFailuresForConversation(string conversationHandle) {
            failuresPerConversationLocker.EnterWriteLock();
            try {
                if (!failuresPerConversation.ContainsKey(conversationHandle))
                    failuresPerConversation[conversationHandle] = 1;
                else
                    failuresPerConversation[conversationHandle] = failuresPerConversation[conversationHandle] + 1;
            } finally {
                failuresPerConversationLocker.ExitWriteLock();
            }
        }

        private bool OnFinishedMessageProcessing() {
            try {
                if (FinishedMessageProcessing != null)
                    FinishedMessageProcessing(this, null);
            } catch (Exception e) {
                Logger.Error("Failed raising 'finished message processing' event.", e);
                return false;
            }

            return true;
        }

        private bool OnTransportMessageReceived(TransportMessage msg) {
            try {
                if (TransportMessageReceived != null)
                    TransportMessageReceived(this, new TransportMessageReceivedEventArgs(msg));
            } catch (Exception e) {
                Logger.Warn("Failed raising 'transport message received' event for message with ID=" + msg.Id, e);
                return false;
            }

            return true;
        }

        private bool OnFailedMessageProcessing() {
            try {
                if (FailedMessageProcessing != null)
                    FailedMessageProcessing(this, null);
            } catch (Exception e) {
                Logger.Warn("Failed raising 'failed message processing' event.", e);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Moves the given message to the configured error queue.
        /// </summary>
        /// <param name="m"></param>
        protected void MoveToErrorQueue(SqlTransaction transaction, Message m) {
            //m.Label = m.Label +
            //          string.Format("<{0}>{1}</{0}><{2}>{3}<{2}>", FAILEDQUEUE, MsmqUtilities.GetIndependentAddressForQueue(queue), ORIGINALID, m.Id);

            //var conversationHandle = ServiceBrokerWrapper.BeginConversation(transaction, InputQueue, ErrorService, ""
            //ServiceBrokerWrapper.Send(transaction, conversationHandle, "", m.Body);
            //ServiceBrokerWrapper.EndConversation(transaction, conversationHandle);

            throw new NotImplementedException();
        }

        private SqlServiceBrokerTransactionManager GetSqlTransactionManager() {
            if (transactionManager == null)
                transactionManager = new SqlServiceBrokerTransactionManager(ConnectionString);
            return transactionManager;
        }

        #region IDisposable Members

        public void Dispose() {
            throw new NotImplementedException();
        }

        #endregion


    }
}
