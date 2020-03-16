using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Moex.Marketplace.Api.Exceptions;
using Moex.Marketplace.Ignite.Models;
using Moex.Marketplace.Ignite.Services.Interfaces;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Moex.Marketplace.Api.Controllers
{
    [ServiceFilter(typeof(ApiExceptionFilter))]
    [Produces("application/json")]
    [Route("api/[controller]")]
    public class CouponsController : Controller
    {
        private readonly IDataService<CouponData> _couponsService;

        public class LowercaseContractResolver : DefaultContractResolver
        {
            protected override string ResolvePropertyName(string propertyName)
            {
                return propertyName.ToLower();
            }
        }

        public CouponsController(IDataService<CouponData> couponsService)
        {
            _couponsService = couponsService;
        }

        [HttpGet("{secid}")]
        [ResponseCache(Duration = 3600)]
        public async Task<IActionResult> Get(string secid)
        {
            if (string.IsNullOrWhiteSpace(secid))
            {
                throw new BadRequestException(nameof(secid));
            }

            var settings = new JsonSerializerSettings
            {
                DateFormatString = "yyyy'-'MM'-'dd",
                ContractResolver = new LowercaseContractResolver()
            };

            return Json(await _couponsService.FindAsync(secid), settings);
        }
    }
}
