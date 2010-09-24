using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NServiceBus.Unicast.Transport.ServiceBroker.Config;

namespace NServiceBus {
    public static class ConfigureServiceBrokerTransport {
        public static ConfigServiceBrokerTransport ServiceBrokerTransport(this Configure config) {
            var cfg = new ConfigServiceBrokerTransport();
            cfg.Configure(config);

            return cfg;
        }
    }
}
