ServiceBroker.Net
========

*An NServiceBus ITransport implementation for SQL Server Service Broker*

About
-----

SQL Server Service Broker is a centralized message queuing subsystem that is provided by SQL Server. It uses logical **services** which accept messages and store them in a **queue**. Applications for that logical service can receive messages from the queue. Services are tied to **message contracts** which define which **message types** the service is able to send and/or receive.

NServiceBus is distrubted message bus application framework. It uses a distributed bus architecture in that when an application sends a message on the bus it is the responsibility of the bus to determine the correct endpoint(s) to deliever the message to. NServiceBus relies on the underlying ITransport to physically deliver the message. It lends itself well to queue based messaging subsystems, such as MSMQ or Service Broker.

Usage
-----

Using the Service Broker ITransport requires both setup of your Service Broker schema and configuration for NServiceBus.

Currently the transport uses a specific message type and message contract when sending messages via Service Broker:

    CREATE MESSAGE TYPE NServiceBusTransportMessage
        VALIDATION = NONE ;
    GO

    CREATE CONTRACT NServiceBusTransportMessageContract
        ( NServiceBusTransportMessage SENT BY ANY);
    GO
    
Afterwhich you can setup your services and queues, etc.:

    CREATE QUEUE [dbo].[ServiceAQueue];
    GO

    CREATE SERVICE ServiceA
        ON QUEUE [dbo].[ServiceAQueue]
        (NServiceBusTransportMessageContract);
    GO
    
The configuration for NServiceBus requires at a minimum the following:

 - ReturnService -- The name of the logical Service Broker service which accepts messages for your application service
 - InputQueue -- The name of the queue to receive messages
 - ErrorService -- The name of a logical Service Broker service to send poison messages
 - ConnectionString -- The SQL Connection string to the database instance for Service Broker
 - MaxRetries -- The maximum number of attempts to process a message before it is sent to the ErrorService
 - NumberOfWorkThreads -- The number of worker threads that will listen for messages on your InputQueue
 
Optionally you can also tune *SecondsToWaitForMessage* which specifies how long the timeout should be for the SQL RECEIVE WAIT command.

Service Broker Automatic Poison Message Detection
-------------------------------------------------

SQL Service Broker includes an automatic poison message detection mechanism, which is always on by default. It works by detecting rolled back transactions that occur when a RECEIVE is used on a queue. If it detects 5 consecutive rollbacks for a queue (such as retrying an errored message), then that queue is DISABLED. This requires an administrator to ENABLE the queue before messages can be received again by the application.

Obviously this is unacceptable for use with NServiceBus. Fortunately SQL Server 2008 R2 provides configuration to turn off this mechanism on a per-queue basis. Unfortunately it is only provided by SQL Server 2008 R2.


Contact
-------

Please e-mail **joseph[at]cridion.com** for feedback, comments, or questions.
