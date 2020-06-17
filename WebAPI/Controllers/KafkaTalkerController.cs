using System;
using System.Text;
using System.Web.Http;
using KafkaNet;
using KafkaNet.Model;

namespace WebAPI.Controllers
{
    [RoutePrefix("kafka")]
    public class KafkaTalkerController : ApiController
    {   
        [HttpGet]
        [Route("Get")]
        public IHttpActionResult GetFromKafa(string myMessage)
        {
            string topic = "Pizza";
            string message = string.Empty;
            Uri uri = new Uri(@"http://localhost:9092");
            var options = new KafkaOptions(uri);
            BrokerRouter brokerRouter = new BrokerRouter(options);
            Consumer kafkaConsumer = new Consumer(new ConsumerOptions(topic, brokerRouter));

            foreach (var messages in kafkaConsumer.Consume())
            {
                message = message + Encoding.UTF8.GetString(messages.Value);
                break;
            }
            return Ok(message);
        }
    }
}
