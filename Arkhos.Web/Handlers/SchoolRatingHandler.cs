using System.Net.Http.Json;
using System.Text;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolRating;
using Arkhos.Core.Requests;
using Arkhos.Core.Requests.SchoolRatings;
using Arkhos.Core.Responses;

namespace Arkhos.Web.Handlers;

public class SchoolRatingHandler(IHttpClientFactory httpClientFactory) : ISchoolRatingsHandler
{
    private readonly HttpClient _client = httpClientFactory.CreateClient(Configuration.HttpClientName);

    public async Task<Response<RegionRatingSummaryDto>> GetRegionRatingSummaryAsync(GetRegionSummaryRequest request)
    {
        var url = new StringBuilder($"v1/schoolrating/summary/region?year={request.Year}");

        if (request.MesorregiaoId.HasValue) url.Append($"&mesorregiaoId={request.MesorregiaoId}");
        if (request.MicrorregiaoId.HasValue) url.Append($"&microrregiaoId={request.MicrorregiaoId}");
        if (request.MunicipioId.HasValue) url.Append($"&municipioId={request.MunicipioId}");
        if (request.Dependencia.HasValue) url.Append($"&dependencia={request.Dependencia}");

        return await _client.GetFromJsonAsync<Response<RegionRatingSummaryDto>>(url.ToString())
               ?? new Response<RegionRatingSummaryDto>(null, 400, "Erro ao obter resumo de avaliações.");
    }

    public async Task<Response<SchoolRatingDetailDto>> GetSchoolDetailAsync(int schoolId, int year)
    {
        var url = $"v1/schoolrating/summary/{schoolId}?year={year}";

        return await _client.GetFromJsonAsync<Response<SchoolRatingDetailDto>>(url)
               ?? new Response<SchoolRatingDetailDto>(null, 400, "Erro ao obter detalhes de avaliação da escola.");
    }

    public async Task<Response<ICollection<SchoolRatingDropDto>>> GetDropByYearAsync(GetSchoolRatingDropByYearRequest request)
    {
        return await _client.GetFromJsonAsync<Response<ICollection<SchoolRatingDropDto>>>($"v1/schoolrating/dropout/{request.Year}")
               ?? new Response<ICollection<SchoolRatingDropDto>>(null, 400, "Erro.");
    }

    public async Task<Response<ICollection<SchoolRatingMapDto>>> GetRatingByYearAsync(GetSchoolRatingMapByYearRequest request)
    {
        return await _client.GetFromJsonAsync<Response<ICollection<SchoolRatingMapDto>>>($"v1/schoolrating/ratings/{request.Year}")
               ?? new Response<ICollection<SchoolRatingMapDto>>(null, 400, "Erro.");
    }

    public async Task<Response<ICollection<SchoolRatingSpendingDto>>> GetSpendingByYearAsync(GetSchoolRatingSpendingByYearRequest request)
    {
        return await _client.GetFromJsonAsync<Response<ICollection<SchoolRatingSpendingDto>>>($"v1/schoolrating/spending/{request.Year}")
               ?? new Response<ICollection<SchoolRatingSpendingDto>>(null, 400, "Erro.");
    }
}