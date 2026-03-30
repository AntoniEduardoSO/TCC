using System.Net.Http.Json;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto;
using Arkhos.Core.Requests.SchoolRatings;
using Arkhos.Core.Responses;

namespace Arkhos.Web.Handlers;

public class SchoolRatingHandler(IHttpClientFactory httpClientFactory) : ISchoolRatingsHandler
{
    private readonly HttpClient _client = httpClientFactory.CreateClient(Configuration.HttpClientName);
    public async Task<Response<ICollection<SchoolRatingSpendingDto>>> GetByYearAsync(GetSchoolRatingByYearRequest request)
    {
        var result = await _client.GetFromJsonAsync<Response<ICollection<SchoolRatingSpendingDto>>>(
            $"v1/schoolrating/spending/{request.Year}"
        );

        return result ?? new Response<ICollection<SchoolRatingSpendingDto>>(null, 400, "Não foi possível obter os schoolratings.");
    }

    public async Task<Response<ICollection<SchoolRatingDropDto>>> GetDropByYearAsync(GetSchoolRatingDropByYearRequest request)
    {
        var result = await _client.GetFromJsonAsync<Response<ICollection<SchoolRatingDropDto>>>(
            $"v1/schoolrating/dropout/{request.Year}"
        );

        return result ?? new Response<ICollection<SchoolRatingDropDto>>(null, 400, "Não foi possível obter os schoolratings.");
    }

    
}