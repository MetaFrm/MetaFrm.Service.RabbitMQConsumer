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
        private IModel? _model;

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

        private void Init()
        {
            this.Close();

            if (string.IsNullOrEmpty(this.ConnectionString))
                return;

            this._connection = new ConnectionFactory
            {
                Uri = new(this.ConnectionString)
            }.CreateConnection();

            this._model = _connection.CreateModel();
            this._model.QueueDeclare(queue: this.QueueName, durable: false, exclusive: false, autoDelete: false, arguments: null);

            var consumer = new EventingBasicConsumer(this._model);
            consumer.Received += Consumer_Received;

            this._model.BasicConsume(queue: this.QueueName, autoAck: true, consumer: consumer);
        }

        private void Consumer_Received(object? sender, BasicDeliverEventArgs e)
        {
            var body = e.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            if (!this.GetAttribute("BrokerService").IsNullOrEmpty())
                ((IServiceString?)this.CreateInstance("BrokerService"))?.Request(message);
            //((IServiceString?)new MetaFrm.Service.BrokerService())?.Request(message);

            this.Action?.Invoke(this, new() { Action = "Consumer_Received", Value = message });
        }
        private void Close()
        {
            if (_model != null && _model.IsOpen)
            {
                _model.Close();
                _model = null;
            }
            if (_connection != null && _connection.IsOpen)
            {
                _connection.Close();
                _connection = null;
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