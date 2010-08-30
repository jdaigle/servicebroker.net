using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using StructureMap;
using NServiceBus;
using System.Threading;

namespace TestRunner {
    class Program {
        static void Main(string[] args) {
            var container = new Container();

            var bus =
            NServiceBus.Configure.With()
                .Log4Net()
                .StructureMapBuilder(container)
                .XmlSerializer()
                .UnicastBus()                
                    .DoNotAutoSubscribe()
                    .LoadMessageHandlers()
                .ServiceBrokerTransport()
                    .ReplyToService("TargetService")
                    .InputQueue("TargetQueue")
                    .ConnectionString(@"Server=.\SQLEXPRESS;Database=ServiceBroker_HelloWorld;Trusted_Connection=True;")
                    .ErrorService("Errors")
                    .IsTransactional(true)
                    .NumberOfWorkerThreads(1)
                    .UseDistributedTransaction(false)
                .CreateBus()
                .Start();

            bus.Send("TargetService", new TestMessage() {
                Content = "Hello World",
            });

            while (true)
                Thread.Sleep(100);
        }
    }

    [Serializable]
    public class TestMessage : IMessage {
        public string Content { get; set; }
    }
}
