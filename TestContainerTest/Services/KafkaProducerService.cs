using Confluent.Kafka;

namespace TestContainerTest.Services;
public class KafkaProducerService
{
    private readonly string _bootstrapServers;
    private readonly string _topic;
    public KafkaProducerService(string bootstrapServers, string topic)
    {
        _bootstrapServers = bootstrapServers;
        _topic = topic;
    }
    public async Task ProduceAsync(string message)
    {
        var config = new ProducerConfig { BootstrapServers = _bootstrapServers };
        using var producer = new ProducerBuilder<Null, string>(config).Build();
        
        try
        {
            var result = await producer.ProduceAsync(_topic, new Message<Null, string> { Value = message });
            Console.WriteLine($"Message '{message}' produced to topic '{_topic}' at offset {result.Offset}");
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Failed to produce message: {e.Error.Reason}");
        }
  }
}
