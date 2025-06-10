
using Confluent.Kafka;
using NSubstitute;
using System.Collections.Concurrent;
using Testcontainers.Kafka;
using TestContainerTest.Services;

namespace TestContainerTest.Test;
public class KafkaConsumerTests : IAsyncLifetime
{
  private readonly KafkaContainer _kafkaContainer;
  private const string Topic = "test-topic";
  private readonly ConcurrentBag<string> _receivedMessages = new();

  public KafkaConsumerTests()
  {
    _kafkaContainer = new KafkaBuilder()
      .Build();
  }

  public async Task InitializeAsync() => await _kafkaContainer.StartAsync();
  public async Task DisposeAsync() => await _kafkaContainer.DisposeAsync();


  [Fact]
  public async Task KafkaConsumer_ShouldReceiveProducedMessage()
  {
    // Arrange
    var bootstrapServers = _kafkaContainer.GetBootstrapAddress();
    var groupId = "test-group";
    var expectedMessage = "Hello from test";

    var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
    using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();
    await producer.ProduceAsync(Topic, new Message<Null, string> { Value = expectedMessage });

    var dummyService = Substitute.For<IDummyService>();
    var kafkaConsumerService = new KafkaConsumerService(bootstrapServers, Topic, groupId, dummyService);
    var cancellationTokenSource = new CancellationTokenSource();

    dummyService.When(x => x.DoSomething(Arg.Any<string>()))
      .Do(callInfo => {
        _receivedMessages.Add(callInfo.Arg<string>());
        cancellationTokenSource.Cancel(); // Cancel after receiving the message to stop the consumer
      });

    // Act
    await kafkaConsumerService.StartConsumingAsync(cancellationTokenSource.Token);

    // Assert
    Assert.Contains(expectedMessage, _receivedMessages);
  }

}
