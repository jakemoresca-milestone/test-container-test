using Confluent.Kafka;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Testcontainers.Kafka;
using TestContainerTest.Services;

namespace TestContainerTest.Test;
public class KafkaProducerServiceTests : IAsyncLifetime
{
  private readonly KafkaContainer _kafkaContainer;
  private const string Topic = "test-topic";
  private readonly ConcurrentBag<string> _receivedMessages = new();
  public KafkaProducerServiceTests()
  {
    _kafkaContainer = new KafkaBuilder()
      .Build();
  }

  public async Task InitializeAsync() => await _kafkaContainer.StartAsync();
  public async Task DisposeAsync() => await _kafkaContainer.DisposeAsync();

  [Fact]
  public async Task KafkaProducer_ShouldProduceMessage()
  {
    // Arrange
    var bootstrapServers = _kafkaContainer.GetBootstrapAddress();
    var producerService = new KafkaProducerService(bootstrapServers, Topic);
    var expectedMessage = "Hello from test";
    var groupId = "test-group";

    // Act
    await producerService.ProduceAsync(expectedMessage);

    // Assert
    var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    var consumerTask = Task.Run(async () =>
    {
      var config = new ConsumerConfig
      {
        BootstrapServers = bootstrapServers,
        GroupId = groupId,
        AutoOffsetReset = AutoOffsetReset.Earliest
      };

      using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
      consumer.Subscribe(Topic);

      try
      {
        while (!cts.Token.IsCancellationRequested)
        {
          var result = consumer.Consume(cts.Token);
          _receivedMessages.Add(result.Message.Value);
          break; // Stop after first message
        }
      }
      catch (OperationCanceledException) { }
      finally
      {
        consumer.Close();
      }
    });

    await consumerTask;

    // Assert
    Assert.Contains(expectedMessage, _receivedMessages);

  }
}
