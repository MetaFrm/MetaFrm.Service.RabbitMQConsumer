using MetaFrm.Control;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace MetaFrm.Service
{
    /// <summary>
    /// RabbitMQConsumer
    /// </summary>
    public class RabbitMQConsumer : IAction, IDisposable
    {
        /// <summary>
        /// Action event Handler입니다.
        /// </summary>
        public event MetaFrmEventHandler? Action;

        private string ConnectionString { get; set; }
        private string QueueName { get; set; }

        private IConnection? _connection;
        private IChannel? _channel;

        private readonly IServiceString? _brokerService;

        /// <summary>
        /// RabbitMQConsumer
        /// </summary>
        public RabbitMQConsumer(string connectionString, string queueName)
        {
            this.ConnectionString = connectionString;
            this.QueueName = queueName;

            if(this.ConnectionString.IsNullOrEmpty())
                this.ConnectionString = this.GetAttribute("ConnectionString");

            if (this.QueueName.IsNullOrEmpty())
                this.QueueName = this.GetAttribute("QueueName");

            if (!this.GetAttribute("BrokerService").IsNullOrEmpty())
                this._brokerService = (IServiceString?)this.CreateInstance("BrokerService");
            //this._brokerService = ((IServiceString?)new MetaFrm.Service.BrokerService());

            Task.Run(() => this.Init());
        }

        private async Task Init()
        {
            this.Close();

            if (string.IsNullOrEmpty(this.ConnectionString))
                return;

            this._connection = await new ConnectionFactory
            {
                Uri = new(this.ConnectionString),
                AutomaticRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(10)
            }.CreateConnectionAsync();

            this._channel = await _connection.CreateChannelAsync();
            
            await this._channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);//Prefetch 설정(Consumer 과부하 방지)
            await this._channel.QueueDeclareAsync(queue: this.QueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

            var consumer = new AsyncEventingBasicConsumer(this._channel);
            consumer.ReceivedAsync += async (model, e) =>
            {
                try
                {
                    var message = Encoding.UTF8.GetString(e.Body.ToArray());

                   this._brokerService?.Request(message);

                    this.Action?.Invoke(this, new() { Action = "Consumer_Received", Value = message });

                    //처리 성공 → ACK
                    await this._channel.BasicAckAsync(deliveryTag: e.DeliveryTag, multiple: false);
                }
                catch (Exception exception)
                {
                    if (Factory.Logger.IsEnabled(LogLevel.Error))
                        Factory.Logger.LogError(exception, "Error : {message}", e.Body);

                    //처리 실패 → NACK(재큐잉)
                    await this._channel!.BasicNackAsync(deliveryTag: e.DeliveryTag, multiple: false, requeue: false);
                }
            };

            await this._channel.BasicConsumeAsync(queue: this.QueueName, autoAck: false, consumer: consumer);
        }

        private void Close()
        {
            if (this._channel != null && this._channel.IsOpen)
            {
                this._channel.CloseAsync();
                this._channel = null;
            }
            if (this._connection != null && this._connection.IsOpen)
            {
                this._connection.CloseAsync();
                this._connection = null;
            }
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        /// <summary>
        /// Dispose
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
                this.Close();
        }
    }
}