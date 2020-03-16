using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Moex.Marketplace.Ignite.Exceptions;
using Moex.Marketplace.Ignite.Models;
using Moex.Marketplace.Ignite.Models.Options;
using Moex.Marketplace.Ignite.Models.Umbraco;
using Moex.Marketplace.Ignite.Quartz;
using Moex.Marketplace.Ignite.Services;
using Moex.Marketplace.Ignite.Services.Attributes;
using Moex.Marketplace.Ignite.Services.Interfaces;

namespace Moex.Marketplace.Ignite
{
    public static class ServiceCollectionExtensions
    {
        private static Regex _resolvingRegex = new Regex("{(.+?)}");

        private static string _envVarPrefix = "marketdata_";

        public static IServiceCollection ConfigureResolve<TOptions>(
            this IServiceCollection services,
            IConfiguration config,
            IConfiguration resolveConfig) where TOptions : class
        {
            return services.Configure<TOptions>(options =>
            {
                config.Bind(options);

                var resolvingProps = options.GetType()
                    .GetProperties()
                    .Where(p =>
                        p.PropertyType == typeof(string) &&
                        p.GetCustomAttributes(typeof(ResolveEnvOptionsAttribute), false).Any());

                foreach (var prop in resolvingProps)
                {
                    string value = prop.GetValue(options) as string;
                    if (string.IsNullOrEmpty(value))
                    {
                        var propName = $"{ options.GetType().FullName}.{prop.Name}";
                        throw new InvalidConfigParamResolve(
                            $"Param ${propName} is null and cannot be resolved");
                    }
                    else
                    {
                        string newValue = _resolvingRegex.Replace(value, match =>
                        {
                            var envValue = resolveConfig[match.Groups[1].Value];
                            return envValue ?? match.Value;
                        });
                        prop.SetValue(options, newValue);
                    }
                }
            });
        }

        public static IServiceCollection AddAttributedServices(this IServiceCollection services)
        {
            var attributeTypes = new List<Type>
            {
                typeof(ScopedServiceAttribute),
                typeof(SingletonServiceAttribute),
                typeof(TransientServiceAttribute),
            };

            var assemblies = AppDomain.CurrentDomain.GetAssemblies();
            var types = assemblies.SelectMany(a =>
                a.GetTypes().SelectMany(type =>
                {
                    var typeAtts = attributeTypes
                        .SelectMany(att => type.GetCustomAttributes(att))
                        .Where(customAtt => customAtt != null);
                    return typeAtts.Select(att => new
                    {
                        type,
                        att,
                    });
                })
                .Where(item => item != null));
            foreach (var item in types)
            {
                var att = item.att as ServiceCollectionServiceAttribute;
                var serviceType = att.ServiceType ?? item.type;
                var implType = item.type;
                var lifetime = att.Lifetime;
                services.Add(new ServiceDescriptor(serviceType, implType, lifetime));
            }

            return services;
        }

        public static void ConfigureIgniteServices(this IServiceCollection services)
        {
            services.AddAttributedServices();

            var cacheItemTypes = new[]
            {
                typeof(Share),
                typeof(ShareFieldSet),
                typeof(ShareHistoryData),
                typeof(Bond),
                typeof(BondFieldSet),
                typeof(BondHistoryData),
                typeof(Etf),
                typeof(EtfFieldSet),
                typeof(Currency),
                typeof(CurrencyFieldSet),
                typeof(CurrencyHistoryData),
                typeof(Futures),
                typeof(FuturesFieldSet),
                typeof(FuturesHistoryData),
                typeof(FuturesContract),
                typeof(Dividend),
                typeof(SecurityStaticData),
                typeof(string),
                typeof(IndexHistoryData),
                typeof(CouponData),
                typeof(TimeStampHistoryData),
                typeof(Indicator),
                typeof(ExchangerPricePair),
                typeof(EmitterProperties),
                typeof(AuthResponce),
                typeof(NodeRole),
                typeof(SecuritySplit),
            };
            foreach (var type in cacheItemTypes)
            {
                var serviceType = typeof(ICacheProvider<>).MakeGenericType(type);
                var implementationType = typeof(IgniteCacheProvider<>).MakeGenericType(type);
                services.AddScoped(serviceType, implementationType);
            }

            services.AddHostedService<QuartzService>();

            var totalConfiguration = new ConfigurationBuilder()
                .AddJsonFile("settings.json")
                .Build();
            var configs = totalConfiguration.GetSection("configs");
            var envPath = configs.GetSection("env").Value;
            var mainPath = configs.GetSection("main").Value;
            var restPath = configs.GetSection("rest").Value;
            var keywordsPath = configs.GetSection("keywords").Value;

            var envConf = new ConfigurationBuilder()
                .AddJsonFile(envPath)
                .AddEnvironmentVariables(_envVarPrefix)
                .Build();

            var restConf = new ConfigurationBuilder()
                .AddJsonFile(restPath)
                .AddEnvironmentVariables(_envVarPrefix)
                .Build();
            var keywordsConf = new ConfigurationBuilder()
                .AddJsonFile(keywordsPath)
                .AddEnvironmentVariables(_envVarPrefix)
                .Build();
            var mainConf = new ConfigurationBuilder()
                .AddJsonFile(mainPath)
                .AddEnvironmentVariables(_envVarPrefix)
                .Build();

            services.ConfigureResolve<RestConfiguration>(restConf, envConf);
            services.Configure<KeywordsConfiguration>(keywordsConf);
            services.ConfigureResolve<HistoryOptions<Share>>(mainConf.GetSection("history"), envConf);
            services.ConfigureResolve<HistoryOptions<Bond>>(mainConf.GetSection("history"), envConf);
            services.ConfigureResolve<HistoryOptions<Currency>>(mainConf.GetSection("history"), envConf);
            services.ConfigureResolve<HistoryOptions<Futures>>(mainConf.GetSection("history"), envConf);
            services.ConfigureResolve<HistoryOptions<Index>>(mainConf.GetSection("history"), envConf);
            services.ConfigureResolve<DividendOptions>(mainConf.GetSection("dividends"), envConf);
            services.ConfigureResolve<MarketOptions<Share>>(mainConf.GetSection("market"), envConf);
            services.ConfigureResolve<MarketOptions<Bond>>(mainConf.GetSection("market"), envConf);
            services.ConfigureResolve<MarketOptions<Currency>>(mainConf.GetSection("market"), envConf);
            services.ConfigureResolve<MarketOptions<Futures>>(mainConf.GetSection("market"), envConf);
            services.ConfigureResolve<ExternalRatesOptions>(mainConf.GetSection("externalRates"), envConf);
            services.ConfigureResolve<BondizationOptions>(mainConf.GetSection("bondization"), envConf);
            services.ConfigureResolve<StaticDataOptions>(mainConf.GetSection("staticData"), envConf);
            services.ConfigureResolve<SplitOptions>(mainConf.GetSection("splits"), envConf);
            services.ConfigureResolve<IgniteClientOptions>(mainConf.GetSection("ignite"), envConf);
            services.ConfigureResolve<IssPlusConfiguration>(mainConf.GetSection("issplus"), envConf);
            services.ConfigureResolve<IndicatorsConfiguration>(mainConf.GetSection("indicators"), envConf);
            services.ConfigureResolve<MinuteHistoryOptions>(mainConf.GetSection("minuteHistory"), envConf);
            services.AddHttpClient<ILuceneHttpClient, LuceneHttpClient>();
            services.AddHttpClient(HttpClientService.HttpClientName);
        }
    }
}
