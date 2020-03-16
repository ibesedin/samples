using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using Microsoft.AspNetCore.Mvc;
using Moex.Marketplace.Api.Exceptions;
using Moex.Marketplace.Api.Tools;
using Moex.Marketplace.Ignite.Models;
using Moex.Marketplace.Ignite.Services.Interfaces;
using Newtonsoft.Json;

namespace Moex.Marketplace.Api.Controllers
{
    [ServiceFilter(typeof(ApiExceptionFilter))]
    [Produces("application/json")]
    public class SecuritiesController : Controller
    {
        private readonly IDataService<Share> _shareService;
        private readonly IDataService<Bond> _bondService;
        private readonly IDataService<Etf> _etfService;
        private readonly IDataService<Currency> _currencyService;
        private readonly IDataService<Futures> _futuresService;

        public SecuritiesController(
            IDataService<Share> shareService,
            IDataService<Bond> bondService,
            IDataService<Etf> etfService,
            IDataService<Currency> currencyService,
            IDataService<Futures> futuresService)
        {
            _bondService = bondService;
            _shareService = shareService;
            _etfService = etfService;
            _currencyService = currencyService;
            _futuresService = futuresService;
        }

        [Route("api/securities")]
        [HttpGet]
        public async Task<IActionResult> GetSecurities(
            string category = "default",
            string query = "",
            string sort = "",
            int length = -1,
            string sortType = "asc")
        {
            var isPublishedPart = $"{nameof(Security.IsPublishedStr)}:{Security.IsPublishedStrValues.Enabled}";
            var publishedOnlyQuery = string.IsNullOrWhiteSpace(query)
                ? isPublishedPart
                : $"{isPublishedPart} AND ({query})";

            var result = await Get(category, publishedOnlyQuery, sort, length, sortType);
            var settings = new JsonSerializerSettings
            {
                ContractResolver = new SecurityWoIsPublishedContractResolver(),
            };
            return Json(result, settings);
        }

        [Route("api/allsecurities")]
        [HttpGet]
        public async Task<IActionResult> GetAllSecurities(
            string category = "default",
            string query = "",
            string sort = "",
            int length = -1,
            string sortType = "asc")
        {
            var result = await Get(category, query, sort, length, sortType);
            return Json(result);
        }

        /// <summary>
        /// Returns securities collection.
        /// </summary>
        /// <param name="category">Filter collection by category. Possible values - null, shares, bonds, etfs, currencies, futures.</param>
        /// <param name="query">Lucene syntax query on securities text fields data. Eg. secId:(SBER SBERP) filter only secs with secId = SBER or SBERP.</param>
        /// <param name="sort">Sorting property name. Used together with sortType.</param>
        /// <param name="length">Response limit.</param>
        /// <param name="sortType">Sort type. Possible values - asc, desc. Used together with sort.</param>
        /// <param name="publishedOnly">Filter published only instruments.</param>
        /// <returns>Array of filtered securities.</returns>
        private async Task<IEnumerable<Security>> Get(
            string category = "default",
            string query = "",
            string sort = "",
            int length = -1,
            string sortType = "asc")
        {
            IEnumerable<Security> result;
            var decodedQuery = HttpUtility.UrlDecode(query);

            if (string.IsNullOrWhiteSpace(sort))
            {
                sortType = "des";
                sort = nameof(Security.Yield);
            }

            switch (category)
            {
                case "shares":
                    result = await Query(_shareService, decodedQuery);
                    break;
                case "bonds":
                    result = await Query(_bondService, decodedQuery);
                    break;
                case "etfs":
                    result = await Query(_etfService, decodedQuery);
                    break;
                case "currencies":
                    result = await Query(_currencyService, decodedQuery);
                    break;
                case "futures":
                    result = await Query(_futuresService, decodedQuery);
                    break;
                case "default":
                    result = (await Task.WhenAll(
                        Query(_shareService, decodedQuery),
                        Query(_bondService, decodedQuery),
                        Query(_etfService, decodedQuery),
                        Query(_currencyService, decodedQuery),
                        Query(_futuresService, decodedQuery)
                    )).SelectMany(_ => _);
                    break;
                default:
                    throw new BadRequestException($"Invalid parameter {nameof(category)} = {category}");
            }

            return EntitySortBuilder<Security>.TakeTopSortedBy(result, sort, sortType, length);
        }

        private async Task<IEnumerable<Security>> Query<T>(IDataService<T> service, string query) where T : Security
        {
            return await(string.IsNullOrEmpty(query) ? service.GetAllAsync() : service.FindAsync(query));
        }
    }
}