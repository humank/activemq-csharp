using Apache.NMS;
using Apache.NMS.ActiveMQ;

namespace consumer;

class Program
{
    static void Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.WriteLine("Please specify consumer identity (e.g., A, B).");
            return;
        }

        const string brokerUri = "failover:(ssl://b-a0f648ab-44c6-4873-87d3-22fbe1049811-1.mq.us-west-2.amazonaws.com:61617,ssl://b-a0f648ab-44c6-4873-87d3-22fbe1049811-2.mq.us-west-2.amazonaws.com:61617)"; // 根据需要修改为你的ActiveMQ服务器地址

        var consumerId = args[0];
        var consumerName = $"Consumer.{consumerId}.VirtualTopic.SampleTopic";
        try
        {
            IConnectionFactory factory = new ConnectionFactory(brokerUri);
            using var connection = factory.CreateConnection("activemq", "activemq2023");
            connection.ClientId = consumerId;
            connection.Start();

            using (var session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
            {
                IDestination destination = session.GetQueue(consumerName + "?consumer.exclusive=true");
                using (var consumer = session.CreateConsumer(destination,null,true))
                {
                    Console.WriteLine($"Consumer {consumerId}: Waiting for messages...");
                    while (consumer.Receive(TimeSpan.FromSeconds(180)) is { } message)
                    {
                        if (message is ITextMessage textMessage)
                        {
                            Console.WriteLine($"Consumer {consumerId}: Received message: {textMessage.Text}");
                        }
                        else
                        {
                            Console.WriteLine($"Consumer {consumerId}: Received non-text message");
                        }
                    }

                    Console.WriteLine($"Consumer {consumerId}: No more messages received, exiting.");
                }
            }

            connection.Stop();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
        }
    }
}