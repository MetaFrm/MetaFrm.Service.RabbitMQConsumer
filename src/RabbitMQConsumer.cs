using MetaFrm.Control;
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

            this.Init();
        }

        private async Task Init()
        {
            await this.Close();

            if (string.IsNullOrEmpty(this.ConnectionString))
                return;

            this._connection = await new ConnectionFactory
            {
                Uri = new(this.ConnectionString)
            }.CreateConnectionAsync();

            this._channel = await _connection.CreateChannelAsync();
            await this._channel.QueueDeclareAsync(queue: this.QueueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

            var consumer = new AsyncEventingBasicConsumer(this._channel);
            consumer.ReceivedAsync += (model, e) =>
            {
                var body = e.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                if (!this.GetAttribute("BrokerService").IsNullOrEmpty())
                    ((IServiceString?)this.CreateInstance("BrokerService"))?.Request(message);
                //((IServiceString?)new MetaFrm.Service.BrokerService())?.Request(message);

                this.Action?.Invoke(this, new() { Action = "Consumer_Received", Value = message });
                return Task.CompletedTask;
            };

            await this._channel.BasicConsumeAsync(queue: this.QueueName, autoAck: true, consumer: consumer);
        }

        private async Task Close()
        {
            if (this._channel != null && this._channel.IsOpen)
            {
                await this._channel.CloseAsync();
                this._channel = null;
            }
            if (this._connection != null && this._connection.IsOpen)
            {
                await this._connection.CloseAsync();
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