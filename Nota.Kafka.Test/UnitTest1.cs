using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.Text;
using kafka4net;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Nota.Kafka.Test
{
    [TestClass]
    public class UnitTest1
    {
        Random _rnd = new Random();
        private string _addresses = "192.168.59.103";
        [TestMethod]
        public async void TestCreateAndConsume()
        {
            var topic = "autocreate.test." + _rnd.Next();
            const int producedCount = 10;
            var wodickh = Encoding.UTF8.GetBytes("wodickh");
            // TODO: set wait to 5sec

            //
            // Produce
            // In order to make sure that topic was created by producer, send and wait for producer
            // completion before performing validation read.
            //
            var producer = new Producer(_addresses, new ProducerConfiguration(topic));

            await producer.ConnectAsync();

            Console.WriteLine("Producing...");
            await Observable.Interval(TimeSpan.FromSeconds(1)).
                Take(producedCount).
                Do(_ => producer.Send(new Message { Value = wodickh })).
                ToTask();
            await producer.CloseAsync(TimeSpan.FromSeconds(10));

            //
            // Validate by reading published messages
            //
            var receivedTxt = new List<string>();
            var complete = new BehaviorSubject<int>(0);
            Action<ReceivedMessage> handler = msg =>
            {
                var str = Encoding.UTF8.GetString(msg.Value);
                lock (receivedTxt)
                {
                    receivedTxt.Add(str);
                    complete.OnNext(receivedTxt.Count);
                }
            };
            var consumer = Consumer.Create(_addresses, topic).
                WithStartPositionAtBeginning().
                WithAction(handler).
                Build();

            Console.WriteLine("Waiting for consumer");
            await complete.TakeWhile(i => i < producedCount).TakeUntil(DateTimeOffset.Now.AddSeconds(5)).LastOrDefaultAsync().ToTask();

            Assert.AreEqual(producedCount, receivedTxt.Count, "Did not receive all messages");
            Assert.IsTrue(receivedTxt.All(m => m == "la-la-la"), "Unexpected message content");

            consumer.Dispose();
        }
    }
}
