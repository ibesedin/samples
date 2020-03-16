using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Moex.Marketplace.Api
{
    public class MetricsService : IHostedService
    {
        private readonly CancellationTokenSource _tokenSource;
        private readonly ILogger _logger;
        private readonly string _message;
        private readonly int _stepCount;
        private readonly TimeSpan _stepInterval;
        
        private Task _task;

        public MetricsService(ILogger<MetricsService> logger)
        {
            _tokenSource = new CancellationTokenSource();
            _logger = logger;
            _message = string.Join(" ", new[]
            {
                "ProcessorCount={processorCount}",
                "WorkingSet64={workingSet64}MiB",
                "PeakWorkingSet64={peakWorkingSet64}MiB",
                "PrivateMemorySize64={privateMemorySize64}MiB",
                "PagedMemorySize64={pagedMemorySize64}MiB",
                "PeakPagedMemorySize64={peakPagedMemorySize64}MiB",
            });
            _stepCount = 10;
            _stepInterval = TimeSpan.FromSeconds(6);
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _task = Task.Run(async () =>
            {
                while (true)
                {
                    LogMemoryStats();

                    for (var i = 0; i < _stepCount; i++)
                    {
                        if (_tokenSource.Token.IsCancellationRequested)
                        {
                            return;
                        }

                        await Task.Delay(_stepInterval);
                    }
                }
            }, _tokenSource.Token);
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _tokenSource.Cancel();
            _task.Wait(TimeSpan.FromSeconds(10));
            return Task.CompletedTask;
        }

        private void LogMemoryStats()
        {
            var process = Process.GetCurrentProcess();
            _logger.LogInformation(
                _message,
                Environment.ProcessorCount,
                GetMebibytes(process.WorkingSet64),
                GetMebibytes(process.PeakWorkingSet64),
                GetMebibytes(process.PrivateMemorySize64),
                GetMebibytes(process.PagedMemorySize64),
                GetMebibytes(process.PeakPagedMemorySize64));
        }

        private float GetMebibytes(long value)
        {
            return value / 1024f / 1024f;
        }
    }
}
