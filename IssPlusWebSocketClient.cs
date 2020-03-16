using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Moex.IssPlusWebSocketClient.Exceptions;
using Moex.IssPlusWebSocketClient.Interfaces;
using Moex.IssPlusWebSocketClient.Stomp;
using Newtonsoft.Json.Linq;
using Headers = System.Collections.Generic.Dictionary<string, string>;

namespace Moex.IssPlusWebSocketClient
{
    public partial class IssPlusWebSocketClient : IDisposable
    {
        private static readonly int _reconnectDelay;
        private static readonly int _maxReconnectsInRow;

        private readonly StompMessageSerializer _ser;
        private readonly ConcurrentDictionary<string, Subscription> _subscriptions;
        private readonly ArraySegment<byte> _buffer;

        private string _sessionId;
        private Task _receiveLoop;
        private ClientWebSocket _client;
        private bool _isReconnecting;

        protected readonly IClientLogger _logger;

        public bool Connected { get; private set; }

        static IssPlusWebSocketClient()
        {
            _reconnectDelay = (int)TimeSpan.FromSeconds(10).TotalMilliseconds;
            _maxReconnectsInRow = 3;
        }

        public IssPlusWebSocketClient(IClientLogger logger)
        {
            _logger = logger;
            Connected = false;
            _subscriptions = new ConcurrentDictionary<string, Subscription>();
            _ser = new StompMessageSerializer();
            _buffer = new ArraySegment<byte>(new byte[1024 * 64]);
        }

        public async Task Add(Subscription sub)
        {
            _subscriptions[sub.Id] = sub;
            if (Connected)
            {
                await Subscribe(sub);
            }
        }

        public async Task Remove(string subscriptionId)
        {
            if (Connected)
            {
                await Unsubscribe(subscriptionId);
            }

            _subscriptions.TryRemove(subscriptionId, out var _);
        }

        public Task Remove(Subscription sub)
        {
            return Remove(sub.Id);
        }

        public async Task Connect(ConnectCredential connectCredential)
        {
            if (Connected)
            {
                _logger.LogWarning("Client already connected");
                return;
            }

            DisposeClient();
            _client = new ClientWebSocket();

            var connectTryNumber = 0;
            while (true)
            {
                try
                {
                    connectTryNumber++;
                    await _client.ConnectAsync(new Uri(connectCredential.Host), CancellationToken.None);
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogException(ex);
                    if (connectTryNumber > 3)
                    {
                        throw;
                    }
                }
            }
            _logger.LogInformation("Connection established");

            var resp = (await Receive()).First().String;
            _sessionId = GetSession(resp);
            _logger.LogInformation($"Session established: {_sessionId}");

            var connectMessage = new StompMessage(StompMessageCommand.CONNECT, new Headers
            {
                ["login"] = connectCredential.Login,
                ["passcode"] = connectCredential.Passcode,
                ["domain"] = connectCredential.Domain,
                ["language"] = "ru",
            });

            await SendMessage(connectMessage);
            foreach (var message in await ReceiveMessages())
            {
                _logger.LogMessage(message);
                ThrowOnErrorStompMessage(message);
            }

            Connected = true;

            foreach (var sub in _subscriptions.Values)
            {
                await Subscribe(sub);
            }

            _receiveLoop = Task.Run(async () =>
            {
                while (Connected)
                {
                    if (_client.State == WebSocketState.Open)
                    {
                        try
                        {
                            foreach (var m in await ReceiveMessages())
                            {
                                _logger.LogMessage(m);
                                ThrowOnErrorStompMessage(m);
                                foreach (var sub in _subscriptions.Values)
                                {
                                    await sub.ProcessMessage(m);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            var standardClosing = ex is IssPlusWebSocketClientReceiveException && ex.Message == "Tunnel closed";
                            if (!standardClosing)
                            {
                                _logger.LogException(ex);
                                var _ = Reconnect(connectCredential);
                                return;
                            }
                        }
                    }
                    else
                    {
                        await Task.Delay(1000);
                    }
                }
            });
        }

        public async Task Disconnect()
        {
            if (!Connected)
            {
                return;
            }

            Connected = false;

            foreach (var sub in _subscriptions.Values)
            {
                try
                {
                    await Unsubscribe(sub.Id);
                }
                catch (Exception ex)
                {
                    _logger.LogException(ex);
                }
            }

            try
            {
                await SendMessage(new StompMessage(StompMessageCommand.DISCONNECT));
            }
            catch (Exception ex)
            {
                _logger.LogException(ex);
            }

            try
            {
                _receiveLoop?.Wait(TimeSpan.FromSeconds(10));
            }
            catch (Exception ex)
            {
                _logger.LogException(ex);
            }

            try
            {
                await _client?.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    string.Empty,
                    CancellationToken.None);
                _logger.LogInformation("Connection closed");
            }
            catch (Exception ex)
            {
                _logger.LogException(ex);
            }
            finally
            {
                DisposeClient();
            }
        }

        private void DisposeClient()
        {
            _client?.Dispose();
            _client = null;
            _logger.LogInformation("Client disposed");
        }

        private async Task Reconnect(ConnectCredential reconnectCredential)
        {
            if (_isReconnecting)
            {
                return;
            }

            _isReconnecting = true;

            int tryNumber = 0;
            while (true)
            {
                try
                {
                    _logger.LogInformation("Performing reconnect");

                    await Task.Delay(_reconnectDelay);

                    await Disconnect();
                    if (reconnectCredential != null)
                    {
                        var subs = _subscriptions.Values.ToArray();
                        foreach (var sub in subs)
                        {
                            await Remove(sub);
                            sub.RenewId();
                            await Add(sub);
                        }

                        await Connect(reconnectCredential);
                    }

                    _logger.LogInformation("Reconnected");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogException(ex);

                    if (tryNumber++ >= _maxReconnectsInRow)
                    {
                        _logger.LogInformation("Reconnect stopped");
                        break;
                    }
                }
            }

            _isReconnecting = false;
        }

        private async Task Subscribe(Subscription sub)
        {
            await SendMessage(sub.GetSubscribeMessage());
        }

        private async Task Unsubscribe(string id)
        {
            var message = new StompMessage(StompMessageCommand.UNSUBSCRIBE, new Headers
            {
                [StompMessageHeader.Id] = id
            });

            await SendMessage(message);
        }

        private string GetSession(string response)
        {
            return JObject.Parse(response).Value<string>("X-CspHub-Session");
        }

        private ArraySegment<byte> GetArraySegment(string message)
        {
            return new ArraySegment<byte>(Encoding.UTF8.GetBytes(message));
        }

        private Task SendMessage(StompMessage message)
        {
            _logger.LogMessage(message);

            try
            {
                return _client.SendAsync(
                    GetArraySegment(_ser.Serialize(message)),
                    WebSocketMessageType.Binary,
                    true,
                    CancellationToken.None);

            }
            catch (AggregateException ex)
            {
                _logger.LogException(ex);
                return Task.CompletedTask;
            }
        }

        private async Task<Queue<ReceivedItem>> GetReceivedQueue()
        {
            return new Queue<ReceivedItem>((await Receive()).Where(item => !string.IsNullOrWhiteSpace(item.String)));
        }

        private void Request(Subscription sub)
        {
            SendMessage(sub.GetRequestMessage());
        }

        private async Task<IEnumerable<StompMessage>> ReceiveMessages()
        {
            var received = await GetReceivedQueue();
            if (!received.Any())
            {
                return Enumerable.Empty<StompMessage>();
            }

            var result = new Queue<StompMessage>();
            while (received.TryDequeue(out var item))
            {
                var message = _ser.Deserialize(item.String);

                while (int.Parse(message.Headers[StompMessageHeader.ContentLength]) > item.Length)
                {
                    while (received.Count == 0)
                    {
                        foreach (var next in await GetReceivedQueue())
                        {
                            received.Enqueue(next);
                        }
                    }

                    received.TryDequeue(out var added);
                    item += added;
                    message = _ser.Deserialize(item.String);
                }

                result.Enqueue(message);
            }

            return result;
        }

        private async Task<IEnumerable<ReceivedItem>> Receive()
        {
            if (_client.State != WebSocketState.Open)
            {
                return Enumerable.Empty<ReceivedItem>();
            }

            var totalReceived = new List<byte>();
            WebSocketReceiveResult receiveResult;
            var result = new List<ReceivedItem>();

            do
            {
                try
                {
                    receiveResult = await _client.ReceiveAsync(_buffer, CancellationToken.None);
                }
                catch (AggregateException ex)
                {
                    throw new IssPlusWebSocketClientException("Invalid receiving", ex);
                }

                if (receiveResult.CloseStatusDescription != null)
                {
                    throw new IssPlusWebSocketClientReceiveException(receiveResult);
                }

                var receivedArray = _buffer.Take(receiveResult.Count).ToArray();
                totalReceived.AddRange(receivedArray);

            } while (!receiveResult.EndOfMessage);

            var split = SplitByZero(totalReceived.ToArray());
            foreach (var piece in split)
            {
                _logger.LogRaw(piece);
                var item = new ReceivedItem(piece);
                result.Add(item);
            }

            return result;
        }

        private IEnumerable<byte[]> SplitByZero(byte[] array)
        {
            int start = 0;
            for (int i = 0; i < array.Length; i++)
            {
                if (array[i] == 0x0)
                {
                    var len = i - start;
                    if (len > 0)
                    {
                        yield return array.Skip(start).Take(len).ToArray();
                    }

                    start = i + 1;
                }
            }

            if (start < array.Length)
            {
                yield return array.Skip(start).ToArray();
            }
        }

        private void ThrowOnErrorStompMessage(StompMessage message)
        {
            if (message.IsError())
            {
                throw new IssPlusWebSocketClientException("STOMP ERROR message");
            }
        }

        public virtual void Dispose()
        {
            DisposeClient();
        }
    }
}
