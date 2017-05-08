using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace AppServicesSvcBusQ
{
    public class Program
    {


        private static QueueClient _queueClient;
        private const string QueueName = "SampleQueue";

        private static readonly string SessionId1 = Guid.NewGuid().ToString();
        private static readonly string SessionId2 = Guid.NewGuid().ToString();
        private static readonly string SessionId3 = Guid.NewGuid().ToString();
        private static readonly string SessionId4 = Guid.NewGuid().ToString();

        static void Main(string[] args)
        {
            if (!VerifyConfiguration())
            {
                Console.ReadLine();
                return;
            }

            Console.WriteLine("Creating a Queue");
            CreateQueue();
            Console.WriteLine("Press any key to start sending messages ...");
            Console.ReadKey();
            SendMessages();
            CheckQueue();
            Console.WriteLine("Press any key to start receiving messages that you just sent.  Press 1 to receive using the IMessageSessionHandler.");
            var c = Console.ReadKey();

            if (c.KeyChar == '1')
                ReceiveMessagesByIMessageSessionHandler();
            else
                ReceiveMessages();

            CheckQueue();
            Console.WriteLine("Press any key to receive messages from the dead letter queue.");
            Console.ReadKey();
            ReceiveMessagesFromDeadLetter();
            CheckQueue();
            Console.WriteLine("\nEnd of scenario, press any key to exit.");
            Console.ReadKey();
        }

        private static bool VerifyConfiguration()
        {
            bool configOk = true;
            var connectionString = ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"];
            if (connectionString.Contains("[your namespace]") || connectionString.Contains("[your access key]"))
            {
                configOk = false;
                Console.WriteLine("Please update the 'Microsoft.ServiceBus.ConnectionString' appSetting in app.config to specify your Service Bus namespace and secret key.");
            }
            return configOk;

        }

        private static void CreateQueue()
        {
            NamespaceManager namespaceManager = NamespaceManager.Create();

            Console.WriteLine("\nCreating Queue '{0}'...", QueueName);

            // Delete if exists
            if (namespaceManager.QueueExists(QueueName))
            {
                namespaceManager.DeleteQueue(QueueName);
            }

            var description = new QueueDescription(QueueName)
            {
                RequiresSession = true
            };

            namespaceManager.CreateQueue(description);
        }

        private static void CheckQueue()
        {
            NamespaceManager namespaceManager = NamespaceManager.Create();

            var queue = namespaceManager.GetQueue(QueueName);

            Console.WriteLine("Queue {0} has a message count of {1} and a dead letter count of {2}.", QueueName, queue.MessageCountDetails.ActiveMessageCount, queue.MessageCountDetails.DeadLetterMessageCount);

        }

        private static void SendMessages()
        {
            _queueClient = QueueClient.Create(QueueName);

            List<BrokeredMessage> messageList = new List<BrokeredMessage>();

            messageList.Add(CreateSampleMessage("1", "First message information sent to session 1", SessionId1));
            messageList.Add(CreateSampleMessage("2", "Second message information sent to session 2", SessionId2));
            messageList.Add(CreateSampleMessage("3", "Third message information sent to session 1", SessionId1));
            messageList.Add(CreateSampleMessage("4", "Fourth message information sent to session 4", SessionId4));
            messageList.Add(CreateSampleMessage("5", "Fifth message information sent to session 3", SessionId3));
            messageList.Add(CreateSampleMessage("6", "Sixth message information sent to session 2", SessionId2));
            messageList.Add(CreateSampleMessage("7", "Seventh message information sent to session 1", SessionId1));
            messageList.Add(CreateSampleMessage("8", "Eighth message information sent to session 4", SessionId4));
            messageList.Add(CreateSampleMessage("9", "Nineth message information sent to session 3", SessionId3));


            Console.WriteLine("\nSending messages to Queue...");

            foreach (BrokeredMessage message in messageList)
            {
                while (true)
                {
                    try
                    {
                        _queueClient.Send(message);
                    }
                    catch (MessagingException e)
                    {
                        if (!e.IsTransient)
                        {
                            Console.WriteLine(e.Message);
                            throw;
                        }
                        HandleTransientErrors(e);
                    }
                    Console.WriteLine("Message sent: Id = {0}, Body = {1}", message.MessageId, message.GetBody<string>());
                    break;
                }
            }

        }

        private static void ReceiveMessages()
        {
            Console.WriteLine("\nReceiving message from Queue...");

            var sessions = _queueClient.GetMessageSessions();

            foreach (var browser in sessions)
            {
                Console.WriteLine("Session discovered: Id = {0}", browser.SessionId);
                try
                {
                    var session = _queueClient.AcceptMessageSession(browser.SessionId);
                    while (true)
                    {
                        try
                        {
                            BrokeredMessage message = session.Receive(TimeSpan.FromSeconds(5));

                            if (message != null)
                            {
                                Console.WriteLine("Message received: Id = {0}, Body = {1}", message.MessageId, message.GetBody<string>());

                                if (session.SessionId == SessionId2)
                                    // if this is the second session then let's send to the dead letter for fun
                                    message.DeadLetter();
                                else
                                    // Further custom message processing could go here...
                                    message.Complete();
                            }
                            else
                            {
                                //no more messages in the queue
                                break;
                            }
                        }
                        catch (MessagingException e)
                        {
                            if (!e.IsTransient)
                            {
                                Console.WriteLine(e.Message);
                                throw;
                            }
                            HandleTransientErrors(e);
                        }
                    }
                }
                catch (Exception)
                {
                    //Do nothing - Just Continue
                }
            }
            _queueClient.Close();
        }

        private static void ReceiveMessagesFromDeadLetter()
        {
            Console.WriteLine("\nReceiving message from Dead Letter Queue...");
            

            var deadLetterPath = QueueClient.FormatDeadLetterPath(QueueName);

            var deadLetterClient = QueueClient.Create(deadLetterPath);

            while (true)
            {
                try
                {
                    BrokeredMessage message = deadLetterClient.Receive(TimeSpan.FromSeconds(5));

                    if (message != null)
                    {
                        Console.WriteLine("Message received: Id = {0}, Body = {1}", message.MessageId, message.GetBody<string>());
                        // Further custom message processing could go here...
                        message.Complete();
                    }
                    else
                    {
                        //no more messages in the queue
                        break;
                    }
                }
                catch (MessagingException e)
                {
                    if (!e.IsTransient)
                    {
                        Console.WriteLine(e.Message);
                        throw;
                    }
                    HandleTransientErrors(e);
                }

            }
            _queueClient.Close();
        }

        private static void ReceiveMessagesByIMessageSessionHandler()
        {
            Console.WriteLine("\nReceiving message from Queue...");

            _queueClient.RegisterSessionHandler(typeof(MyMessageSessionHandler), new SessionHandlerOptions { AutoComplete = false });

            NamespaceManager namespaceManager = NamespaceManager.Create();
            var queue = namespaceManager.GetQueue(QueueName);

            while (queue.MessageCount > 0)
            {
                Thread.CurrentThread.Join(100);
                queue = namespaceManager.GetQueue(QueueName);
            }

            _queueClient.Close();
        }


        private static BrokeredMessage CreateSampleMessage(string messageId, string messageBody, string sessionId)
        {
            BrokeredMessage message = new BrokeredMessage(messageBody);
            message.MessageId = messageId;
            message.SessionId = sessionId;
            return message;
        }

        private static void HandleTransientErrors(MessagingException e)
        {
            //If transient error/exception, let's back-off for 2 seconds and retry
            Console.WriteLine(e.Message);
            Console.WriteLine("Will retry sending the message in 2 seconds");
            Thread.Sleep(2000);
        }
    }

    public class MyMessageSessionHandler : IMessageSessionHandler
    {
        private readonly string _whoAmI = Guid.NewGuid().ToString().Substring(0, 4);

        public void OnCloseSession(MessageSession session)
        {
            Console.WriteLine("MyMessageSessionHandler {1} close session Id = {0}", session.SessionId, _whoAmI);
        }

        public void OnMessage(MessageSession session, BrokeredMessage message)
        {
            Console.WriteLine("MyMessageSessionHandler {3} received messaged on session Id = {0}, Id = {1}, Body = {2}", session.SessionId, message.MessageId, message.GetBody<string>(), _whoAmI);

            message.Complete();
        }

        public void OnSessionLost(Exception exception)
        {
            Console.WriteLine("MyMessageSessionHandler {1} OnSessionLost {0}", exception.Message, _whoAmI);
        }
    }

}
