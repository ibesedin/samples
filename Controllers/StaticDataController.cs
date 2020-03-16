using System.Threading.Tasks;
using System.Web;
using Microsoft.AspNetCore.Mvc;
using Moex.Marketplace.Ignite.Services;

namespace Moex.Marketplace.Api.Controllers
{
    [ServiceFilter(typeof(ApiExceptionFilter))]
    [Produces("application/json")]
    [Route("api/[controller]")]
    public class StaticDataController : Controller
    {
        private readonly SecurityStaticDataService _staticDataService;

        public StaticDataController(
            SecurityStaticDataService staticDataService)
        {
            _staticDataService = staticDataService;
        }

        [HttpGet]
        [ResponseCache(Duration = 180, VaryByQueryKeys = new[] { "query" })]
        public async Task<IActionResult> Get(string query = "")
        {
            var decodedQuery = HttpUtility.UrlDecode(query);
            var result = await _staticDataService.Query(decodedQuery);
            return Json(result);
        }
    }
}
