using System;
using NServiceBus.Config;
using NServiceBus.ObjectBuilder;
using System.Data;
using System.Configuration;

namespace NServiceBus.Unicast.Transport.ServiceBroker.Config {
    public class ConfigServiceBrokerTransport : Configure {

        /// <summary>
        /// Wraps the given configuration object but stores the same 
        /// builder and configurer properties.
        /// </summary>
        /// <param name="config"></param>
        public void Configure(Configure config) {
            Builder = config.Builder;
            Configurer = config.Configurer;

            transportConfig = Configurer.ConfigureComponent<ServiceBrokerTransport>(ComponentCallModelEnum.Singleton);

            var cfg = GetConfigSection<ServiceBrokerTransportConfig>();            

            if (cfg != null) {
                transportConfig.ConfigureProperty(t => t.InputQueue, cfg.InputQueue);
                transportConfig.ConfigureProperty(t => t.NumberOfWorkerThreads, cfg.NumberOfWorkerThreads);
                transportConfig.ConfigureProperty(t => t.ErrorService, cfg.ErrorService);
                transportConfig.ConfigureProperty(t => t.MaxRetries, cfg.MaxRetries);
                ConnectionString(ConfigurationManager.ConnectionStrings[cfg.ConnectionStringName].ConnectionString);
                ReplyToService(cfg.ReplyToService);
            }
        }

        private IComponentConfig<ServiceBrokerTransport> transportConfig;

        public ConfigServiceBrokerTransport ReplyToService(string value) {
            transportConfig.ConfigureProperty(t => t.ReplyToService, value);
            return this;
        }

        public ConfigServiceBrokerTransport ConnectionString(string value) {
            transportConfig.ConfigureProperty(t => t.ConnectionString, value);
            return this;
        }

        public ConfigServiceBrokerTransport InputQueue(string value) {
            transportConfig.ConfigureProperty(t => t.InputQueue, value);
            return this;
        }

        public ConfigServiceBrokerTransport NumberOfWorkerThreads(int value) {
            transportConfig.ConfigureProperty(t => t.NumberOfWorkerThreads, value);
            return this;
        }

        public ConfigServiceBrokerTransport ErrorService(string value) {
            transportConfig.ConfigureProperty(t => t.ErrorService, value);
            return this;
        }

        public ConfigServiceBrokerTransport MaxRetries(int value) {
            transportConfig.ConfigureProperty(t => t.MaxRetries, value);
            return this;
        }

        /// <summary>
        /// Sets the transactionality of the endpoint.
        /// If true, the endpoint will not lose messages when exceptions occur.
        /// If false, the endpoint may lose messages when exceptions occur.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public ConfigServiceBrokerTransport IsTransactional(bool value) {
            transportConfig.ConfigureProperty(t => t.IsTransactional, value);
            return this;
        }


        public ConfigServiceBrokerTransport UseDistributedTransaction(bool value) {
            transportConfig.ConfigureProperty(t => t.UseDistributedTransaction, value);
            return this;
        }

        public ConfigServiceBrokerTransport SecondsToWaitForMessage(int value) {
            transportConfig.ConfigureProperty(t => t.SecondsToWaitForMessage, value);
            return this;
        }
        

        /// <summary>
        /// Sets the isolation level that database transactions on this endpoint will run at.
        /// This value is only relevant when IsTransactional has been set to true.
        /// 
        /// Higher levels like RepeatableRead and Serializable promise a higher level
        /// of consistency, but at the cost of lower parallelism and throughput.
        /// 
        /// If you wish to run sagas on this endpoint, RepeatableRead is the suggested value
        /// and is the default value.
        /// </summary>
        /// <param name="isolationLevel"></param>
        /// <returns></returns>
        public ConfigServiceBrokerTransport IsolationLevel(IsolationLevel isolationLevel) {
            transportConfig.ConfigureProperty(t => t.IsolationLevel, isolationLevel);
            return this;
        }

        /// <summary>
        /// Sets the time span where a transaction will timeout.
        /// 
        /// Most endpoints should leave it at the default.
        /// </summary>
        /// <param name="transactionTimeout"></param>
        /// <returns></returns>
        public ConfigServiceBrokerTransport TransactionTimeout(TimeSpan transactionTimeout) {
            transportConfig.ConfigureProperty(t => t.TransactionTimeout, transactionTimeout);
            return this;
        }
    }
}
