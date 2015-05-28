using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using kafka4net;

namespace SimpleConsoleConsumer
{
    class Program
    {
        private const string brokerIP = "192.168.59.103";
        static void Main(string[] args)
        {
            Program program = new Program();
            program.ConsumeMessages("topic2");
        }

        private void ConsumeMessages(string topic)
        {
            var receivedEvents = new List<int>(100);
            var handler = new BufferBlock<ReceivedMessage>();
            var consumer = Consumer.Create(brokerIP, topic).
                WithStartPositionAtBeginning().
                WithAction(handler).
                Build();
            var msgs = handler.AsObservable().Publish().RefCount();
         //   var t1 = msgs.TakeUntil(DateTimeOffset.Now.AddSeconds(5)).LastOrDefaultAsync().ToTask();
       //     var t2 = msgs.TakeUntil(DateTimeOffset.Now.AddSeconds(6)).LastOrDefaultAsync().ToTask();
            //await consumer.IsConnected;
            var count2 = await handler.AsObservable()
                .Do(msg =>
                {
                    var value = BitConverter.ToInt32((msg.Value, 0);
                    Console.WriteLine("Consumer received value {0} from partition {1} at offset {2}", value,
                        msg.Partition, msg.Offset);
                    receivedEvents.Add(value);

                }).Take(50);

           // await Task.WhenAll(new[] { t1, t2 });
            consumer.Dispose();
        }

   /*     private async void ProduceAndConsume(string topic)
        {
            const int count = 100;
            var topic = topic;
        //    VagrantBrokerUtil.CreateTopic(topic, 3, 2);

            // fill it out with 100 messages
            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));
            await producer.ConnectAsync();

            Console.WriteLine("Sending data");
            Enumerable.Range(1, count).
                Select(i => new Message { Value = BitConverter.GetBytes(i) }).
                ForEach(producer.Send);

            Console.WriteLine("Closing producer");
            await producer.CloseAsync(TimeSpan.FromSeconds(5));

            // read starting from the head
            var targetMessages = new BufferBlock<ReceivedMessage>();
            var consumer = Consumer.Create(_seedAddresses, topic).
                WithStartPositionAtBeginning().
                WithAction(targetMessages).
                Build();
            var count2 = await targetMessages.AsObservable().TakeUntil(DateTimeOffset.Now.AddSeconds(5))
                //.Do(val=>_log.Info("received value {0}", BitConverter.ToInt32(val.Value,0)))
                .Count().ToTask();
        }
    * */
        private void ProduceMessage()
        {
            
        }
    }
}
