using System.Net.Http.Json;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.CityInfo;
using Arkhos.Core.Requests.CityInfos;
using Arkhos.Core.Responses;

namespace Arkhos.Web.Handlers;

public class CityInfosHandler(IHttpClientFactory httpClientFactory) : ICityInfosHandler
{
    private readonly HttpClient _client = httpClientFactory.CreateClient(Configuration.HttpClientName);

    public async Task<Response<ICollection<CityInfoMapDto>>> GetByYearAsync(GetCityInfosByYearRequest request)
    {
        var result = await _client.GetFromJsonAsync<Response<ICollection<CityInfoMapDto>>>(
            $"v1/cityinfo/{request.Year}"
        );

        return result ?? new Response<ICollection<CityInfoMapDto>>(null, 400, "Não foi possível obter os cityinfos.");
    }
}