using System;

namespace ServiceBrokerDotNet
{
    public class PoisonMessageInfo
    {
        public Guid MessageId { get; set; }
        public DateTime InsertDateTime { get; set; }
        public string Queue { get; set; }
        public string QueueService { get; set; }
        public string OriginService { get; set; }
    }
}
