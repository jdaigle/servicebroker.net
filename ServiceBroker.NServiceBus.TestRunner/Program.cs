﻿using System;
using System.Threading;
using NServiceBus;
using NServiceBus.Unicast.Transport;
using StructureMap;

namespace TestRunner {
    class Program {
        static void Main(string[] args) {
            System.Transactions.TransactionManager.DistributedTransactionStarted += new System.Transactions.TransactionStartedEventHandler(TransactionManager_DistributedTransactionStarted);
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
                    .ReturnService("ServiceA")
                    .InputQueue("ServiceAQueue")
                    .ConnectionString(@"Server=.\SQLEXPRESS;Database=ServiceBroker_HelloWorld;Trusted_Connection=True;")
                    .ErrorService("ErrorService")
                    .MaxRetries(2)
                    .NumberOfWorkerThreads(2)
                .CreateBus()
                .Start();

            bus.Send("ServiceA", new TestMessage() {
                Content = "Hello World - Send()",
            }).Register(x => {
                Console.WriteLine(x);
            });

            bus.SendLocal(new TestMessage() {
                Content = "Hello World - SendLocal()",
            });

            var count = NServiceBus.Configure.Instance.Builder.Build<ITransport>().GetNumberOfPendingMessages();

            while (true)
                Thread.Sleep(100);
        }

        static void TransactionManager_DistributedTransactionStarted(object sender, System.Transactions.TransactionEventArgs e) {
            Console.WriteLine("Distributed Transaction Started");
        }
    }

    [Serializable]
    public class TestMessage : IMessage {
        public string Content { get; set; }
    }

    public class TestMessageHandler : IMessageHandler<TestMessage> {

        public IBus Bus { get; set; }

        public void Handle(TestMessage message) {
            //
            Bus.Return(42);
            throw new Exception("Testing Exception Management");
        }
    }

}
