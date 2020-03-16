using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moex.Marketplace.Ignite.Models.Options;
using Moex.Marketplace.Ignite.Quartz.Attributes;
using Quartz;
using Quartz.Impl;

namespace Moex.Marketplace.Ignite.Quartz
{
    public class QuartzService : IHostedService, IDisposable
    {
        private readonly IServiceProvider _provider;
        private readonly ILogger _logger;
        private readonly string _trSuffix;
        private readonly int _historyJobStartDelay;
        private IScheduler _scheduler;

        public static bool Started { get; private set; } = false;
        public static IServiceProvider Provider { get; private set; }

        public QuartzService(
            IServiceProvider provider,
            ILogger<QuartzService> logger)
        {
            _trSuffix = "Trigger";
            _historyJobStartDelay = (int)TimeSpan.FromSeconds(60).TotalMilliseconds;
            _provider = provider;
            Provider = provider;
            _logger = logger;
        }

        private IJobDetail BuildJob(Type runnerType)
        {
            return JobBuilder
                .Create(runnerType)
                .WithIdentity(Guid.NewGuid().ToString())
                .Build();
        }

        private async Task ScheduleJob(IJobDetail runnerJob, Type serviceType)
        {
            var trigger = GetTrigger(serviceType);
            if (trigger == null)
            {
                await _scheduler.AddJob(runnerJob, true, true);
            }
            else
            {
                await _scheduler.ScheduleJob(runnerJob, trigger);
            }
        }

        private async Task StartJob(IJobDetail job, int timeout = 0)
        {
            await Task.Delay(timeout);
            await _scheduler.TriggerJob(job.Key);
        }

        private Task RemoveJob(IJobDetail job)
        {
            return _scheduler.DeleteJob(job.Key);
        }

        private ITrigger GetTrigger(Type serviceType)
        {
            CronExpressionBase conf = null;

            var atts = serviceType.GetCustomAttributes(typeof(CronExpressionConfigurationAttribute), false);
            if (atts.Any())
            {
                var att = atts.First() as CronExpressionConfigurationAttribute;
                var options = _provider.GetService(att.CronExpressionType) as IOptions<CronExpressionBase>;
                conf = options.Value;
            }

            if (string.IsNullOrEmpty(conf?.CronExpression))
            {
                throw new ApplicationException($"Invalid schedule for job {serviceType.Name}");
            }

            return TriggerBuilder.Create()
                .WithIdentity(serviceType.Name + _trSuffix)
                .WithCronSchedule(conf.CronExpression)
                .Build();
        }

        private async Task StartQuartz()
        {
            if (_scheduler == null)
            {
                var properties = new NameValueCollection
                {
                    ["quartz.scheduler.instanceName"] = "Scheduler",
                    ["quartz.jobStore.type"] = "Quartz.Simpl.RAMJobStore, Quartz",
                    ["quartz.serializer.type"] = "json",
                };

                var factory = new StdSchedulerFactory(properties);
                _scheduler = await factory.GetScheduler();
                //_scheduler.JobFactory = new DiJobFactory(_provider);
            }

            if (!_scheduler.IsStarted)
            {
                await _scheduler.Start();
            }
        }

        private async Task StopQuartz(CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_scheduler == null)
            {
                return;
            }

            var shutdown = _scheduler.Shutdown(waitForJobsToComplete: true, cancellationToken);
            var delay = Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            if (await Task.WhenAny(shutdown, delay) == shutdown)
            {
                _scheduler = null;
            }
            else
            {
                _logger.LogWarning("Jobs didn't exited correctly");
            }

            Started = false;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await StartQuartz();
            await RegisterJobs();
            Started = true;
        }

        private async Task RegisterJobs()
        {
            var startList = new List<JobStartDelay>
            {
                new JobStartDelay(typeof(UpdateIndicatorJob)),
                new JobStartDelay(typeof(UpdateBackendDataJob)),
                new JobStartDelay(typeof(UpdateMinuteHistoryJob)),
                new JobStartDelay(typeof(UpdateHistoryDataJob), _historyJobStartDelay),
                new JobStartDelay(typeof(UpdateDividendsJob)),
                new JobStartDelay(typeof(UpdateCouponsJob)),
                new JobStartDelay(typeof(UpdateSplitsJob)),
                new JobStartDelay(typeof(UpdateFuturesContractsJob), _historyJobStartDelay),
                new JobStartDelay(typeof(UpdateStaticDataJob), _historyJobStartDelay),
            };

            foreach (var startData in startList)
            {
                var runnerType = typeof(JobRunner<>).MakeGenericType(startData.JobType);
                var runnerJob = BuildJob(runnerType);
                await ScheduleJob(runnerJob, startData.JobType);
                _ = StartJob(runnerJob, startData.Delay);
                _logger.LogInformation("Job {jobName} started.", startData.JobType.Name);
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return StopQuartz(cancellationToken);
        }

        public void Dispose()
        {
            StopQuartz().Wait();
        }
    }
}
