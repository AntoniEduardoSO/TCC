using System.Net.Http.Json;
using System.Text;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolInfraValues;
using Arkhos.Core.Requests;
using Arkhos.Core.Requests.SchoolInfraValues;
using Arkhos.Core.Responses;

namespace Arkhos.Web.Handlers;

public class SchoolInfraValuesHandler(IHttpClientFactory httpClientFactory) : ISchoolInfraValuesHandler
{
    private readonly HttpClient _client = httpClientFactory.CreateClient(Configuration.HttpClientName);

    public async Task<Response<RegionInfraSummaryDto>> GetRegionSummaryAsync(GetRegionSummaryRequest request)
    {
        var url = new StringBuilder($"v1/schoolinfravalues/summary/region?year={request.Year}");

        if (request.MesorregiaoId.HasValue) url.Append($"&mesorregiaoId={request.MesorregiaoId}");
        if (request.MicrorregiaoId.HasValue) url.Append($"&microrregiaoId={request.MicrorregiaoId}");
        if (request.MunicipioId.HasValue) url.Append($"&municipioId={request.MunicipioId}");
        if (request.Dependencia.HasValue) url.Append($"&dependencia={request.Dependencia}");

        return await _client.GetFromJsonAsync<Response<RegionInfraSummaryDto>>(url.ToString())
               ?? new Response<RegionInfraSummaryDto>(null, 400, "Erro ao obter resumo de infraestrutura.");
    }

    public async Task<Response<SchoolInfraDetailDto>> GetSchoolDetailAsync(int schoolId, int year)
    {
        var url = $"v1/schoolinfravalues/summary/{schoolId}?year={year}";
        
        return await _client.GetFromJsonAsync<Response<SchoolInfraDetailDto>>(url)
               ?? new Response<SchoolInfraDetailDto>(null, 400, "Erro ao obter detalhes de infraestrutura da escola.");
    }

    public async Task<Response<ICollection<SchoolInfraValuesPedagogicalRoomsDto>>> GetPedagogicalRoomsByYearAsync(GetSchoolInfraValuesPedagogicalRoomsByYearRequest request)
    {
        return await _client.GetFromJsonAsync<Response<ICollection<SchoolInfraValuesPedagogicalRoomsDto>>>($"v1/schoolinfravalues/pedagogicalrooms/{request.Year}")
               ?? new Response<ICollection<SchoolInfraValuesPedagogicalRoomsDto>>(null, 400, "Erro.");
    }

    public async Task<Response<ICollection<SchoolInfraValuesWellbeingDto>>> GetWellbeingByYearAsync(GetSchoolInfraValuesWellbeingByYearRequest request)
    {
        return await _client.GetFromJsonAsync<Response<ICollection<SchoolInfraValuesWellbeingDto>>>($"v1/schoolinfravalues/wellbeing/{request.Year}")
               ?? new Response<ICollection<SchoolInfraValuesWellbeingDto>>(null, 400, "Erro.");
    }
}