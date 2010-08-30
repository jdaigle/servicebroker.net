using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using StructureMap;
using NServiceBus;

namespace TestRunner {
    class Program {
        static void Main(string[] args) {
            var container = new Container();

            var bus =
            NServiceBus.Configure.With()
                .StructureMapBuilder()
                .XmlSerializer()
                .UnicastBus()
                    .DoNotAutoSubscribe()
                    .LoadMessageHandlers()
                .ServiceBrokerTransport()
                    .InputQueue("TargetQueue")
                    .ConnectionString(@"Server=.\SQLEXPRESS;Database=ServiceBroker_HelloWorld;Trusted_Connection=True;")
                    .ErrorService("Errors")
                    .IsTransactional(true)
                    .NumberOfWorkerThreads(1)
                    .UseDistributedTransaction(false)
                .CreateBus()
                .Start();

            bus.Send("InitiatorService", new TestMessage() {
                Content = "Hello World",
            });
        }
    }

    public class TestMessage : IMessage {
        public string Content { get; set; }
    }
}
