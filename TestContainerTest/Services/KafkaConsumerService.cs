using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestContainerTest.Services;
public class KafkaConsumerService(string bootstrapServers, string topic, string groupId, IDummyService dummyService)
{
  public async Task StartConsumingAsync(CancellationToken cancellationToken)
  {
    var config = new ConsumerConfig
    {
      BootstrapServers = bootstrapServers,
      GroupId = groupId,
      AutoOffsetReset = AutoOffsetReset.Earliest
    };

    using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
    consumer.Subscribe(topic);

    try
    {
      while (!cancellationToken.IsCancellationRequested)
      {
        var result = consumer.Consume(cancellationToken);
        Console.WriteLine($"Received message: {result.Message.Value}");
        dummyService.DoSomething(result.Message.Value); // Call the dummy service method
      }
    }
    catch (OperationCanceledException)
    {
      Console.WriteLine("Consumer cancelled.");
    }
    finally
    {
      consumer.Close();
    }
  }

}
