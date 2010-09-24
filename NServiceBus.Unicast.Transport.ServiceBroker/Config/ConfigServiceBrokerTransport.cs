using System;
using System.Transactions;
using NServiceBus.Config;
using NServiceBus.ObjectBuilder;

namespace NServiceBus.Unicast.Transport.ServiceBroker.Config
{
    public class ConfigServiceBrokerTransport : Configure
    {

        /// <summary>
        /// Wraps the given configuration object but stores the same 
        /// builder and configurer properties.
        /// </summary>
        /// <param name="config"></param>
        public void Configure(Configure config)
        {
            Builder = config.Builder;
            Configurer = config.Configurer;

            transportConfig = Configurer.ConfigureComponent<ServiceBrokerTransport>(ComponentCallModelEnum.Singleton);

            var cfg = GetConfigSection<ServiceBrokerTransportConfig>();

            if (cfg != null)
            {
                transportConfig.ConfigureProperty(t => t.InputQueue, cfg.InputQueue);
                transportConfig.ConfigureProperty(t => t.NumberOfWorkerThreads, cfg.NumberOfWorkerThreads);
                transportConfig.ConfigureProperty(t => t.ErrorService, cfg.ErrorService);
                transportConfig.ConfigureProperty(t => t.MaxRetries, cfg.MaxRetries);
                ConnectionString(cfg.ConnectionString);
                ReturnService(cfg.ReturnService);
            }
        }

        private IComponentConfig<ServiceBrokerTransport> transportConfig;

        public ConfigServiceBrokerTransport ReturnService(string value)
        {
            transportConfig.ConfigureProperty(t => t.ReturnService, value);
            return this;
        }

        public ConfigServiceBrokerTransport ConnectionString(string value)
        {
            transportConfig.ConfigureProperty(t => t.ConnectionString, value);
            return this;
        }

        public ConfigServiceBrokerTransport InputQueue(string value)
        {
            transportConfig.ConfigureProperty(t => t.InputQueue, value);
            return this;
        }

        public ConfigServiceBrokerTransport NumberOfWorkerThreads(int value)
        {
            transportConfig.ConfigureProperty(t => t.NumberOfWorkerThreads, value);
            return this;
        }

        public ConfigServiceBrokerTransport ErrorService(string value)
        {
            transportConfig.ConfigureProperty(t => t.ErrorService, value);
            return this;
        }

        public ConfigServiceBrokerTransport MaxRetries(int value)
        {
            transportConfig.ConfigureProperty(t => t.MaxRetries, value);
            return this;
        }

        public ConfigServiceBrokerTransport SecondsToWaitForMessage(int value)
        {
            transportConfig.ConfigureProperty(t => t.SecondsToWaitForMessage, value);
            return this;
        }

        public ConfigServiceBrokerTransport UseDistributedTransaction(bool value)
        {
            transportConfig.ConfigureProperty(t => t.UseDistributedTransaction, value);
            return this;
        }

        public ConfigServiceBrokerTransport IsolationLevel(IsolationLevel value)
        {
            transportConfig.ConfigureProperty(t => t.IsolationLevel, value);
            return this;
        }

        public ConfigServiceBrokerTransport TransactionTimeout(TimeSpan value)
        {
            transportConfig.ConfigureProperty(t => t.TransactionTimeout, value);
            return this;
        }
    }
}
