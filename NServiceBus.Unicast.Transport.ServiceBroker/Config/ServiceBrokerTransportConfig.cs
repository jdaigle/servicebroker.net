using System;
using System.Configuration;

namespace NServiceBus.Config {
    public class ServiceBrokerTransportConfig : ConfigurationSection {

        [ConfigurationProperty("ReturnService", IsRequired = true)]
        public string ReturnService {
            get {
                return this["ReturnService"] as string;
            }
            set {
                this["ReturnService"] = value;
            }
        }

        /// <summary>
        /// The queue to receive messages from in the format
        /// "[database].[schema].[queue]".
        /// </summary>
        [ConfigurationProperty("InputQueue", IsRequired = true)]
        public string InputQueue {
            get {
                return this["InputQueue"] as string;
            }
            set {
                this["InputQueue"] = value;
            }
        }

        /// <summary>
        /// The service to which to forward messages that could not be processed
        /// </summary>
        [ConfigurationProperty("ErrorService", IsRequired = true)]
        public string ErrorService {
            get {
                return this["ErrorService"] as string;
            }
            set {
                this["ErrorService"] = value;
            }
        }

        /// <summary>
        /// The number of worker threads that can process messages in parallel.
        /// </summary>
        [ConfigurationProperty("NumberOfWorkerThreads", IsRequired = true)]
        public int NumberOfWorkerThreads {
            get {
                return (int)this["NumberOfWorkerThreads"];
            }
            set {
                this["NumberOfWorkerThreads"] = value;
            }
        }

        /// <summary>
        /// The maximum number of times to retry processing a message
        /// when it fails before moving it to the error queue.
        /// </summary>
        [ConfigurationProperty("MaxRetries", IsRequired = true)]
        public int MaxRetries {
            get {
                return (int)this["MaxRetries"];
            }
            set {
                this["MaxRetries"] = value;
            }
        }

        [ConfigurationProperty("ConnectionString", IsRequired = true)]
        public string ConnectionString {
            get {
                return (string)this["ConnectionString"];
            }
            set {
                this["ConnectionString"] = value;
            }
        }
    }
}
