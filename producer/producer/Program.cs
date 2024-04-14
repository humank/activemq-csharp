using System;
using Apache.NMS;
using Apache.NMS.ActiveMQ;

class Program
{
    static void Main()
    {
        const string brokerUri = "failover:(ssl://b-a0f648ab-44c6-4873-87d3-22fbe1049811-1.mq.us-west-2.amazonaws.com:61617,ssl://b-a0f648ab-44c6-4873-87d3-22fbe1049811-2.mq.us-west-2.amazonaws.com:61617)";
        const string virtualTopicName = "VirtualTopic.SampleTopic";

        try
        {
            IConnectionFactory factory = new ConnectionFactory(brokerUri);
            using var connection = factory.CreateConnection("activemq", "activemq2023");
            connection.Start();

            using (var session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
            {
                IDestination destination = session.GetTopic(virtualTopicName);
                using (var producer = session.CreateProducer(destination))
                {
                    producer.DeliveryMode = MsgDeliveryMode.Persistent;

                    for (var i = 0; i < 10; i++)
                    {
                        var messageText = $"Message {i}";
                        var message = session.CreateTextMessage(messageText);
                        producer.Send(message);
                        Console.WriteLine($"Sent message: {messageText}");
                    }
                }
            }

            connection.Stop();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }
}