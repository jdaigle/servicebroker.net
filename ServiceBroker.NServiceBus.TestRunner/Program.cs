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
                    .ReplyToService("ServiceA")
                    .InputQueue("ServiceAQueue")
                    .ConnectionString(@"Server=.\SQLEXPRESS;Database=ServiceBroker_HelloWorld;Trusted_Connection=True;")
                    .ErrorService("ServiceB")
                    .IsTransactional(true)
                    .NumberOfWorkerThreads(1)
                    .UseDistributedTransaction(false)
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
            Console.WriteLine(message.Content);
            if (DateTime.Now.Second % 2 == 1) {
                Bus.HandleCurrentMessageLater();
            } else {
                Bus.Return(42);
            }
            
        }
    }

}
