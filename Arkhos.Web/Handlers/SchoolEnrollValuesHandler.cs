using System.Diagnostics;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolEnrollValues;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Responses;

namespace Arkhos.Web.Handlers;
public class SchoolEnrollValuesHandler(IHttpClientFactory httpClientFactory) : ISchoolEnrollValuesHandler
{
    private readonly HttpClient _client = httpClientFactory.CreateClient(Configuration.HttpClientName);

    public async Task<Response<RegionEnrollmentSummaryDto>> GetRegionEnrollmentSummaryByFilterAsync(GetRegionEnrollmentSummaryByFilterRequest request)
    {
        var url = $"v1/schoolenrollvalues/summary/region?year={request.Year}";
    
        if (request.MesorregiaoId.HasValue) url += $"&mesorregiaoId={request.MesorregiaoId}";
        if (request.MicrorregiaoId.HasValue) url += $"&microrregiaoId={request.MicrorregiaoId}";
        if (request.MunicipioId.HasValue) url += $"&municipioId={request.MunicipioId}";
        if (request.Depedencia.HasValue) url += $"&depedencia={request.Depedencia}";

        return await _client.GetFromJsonAsync<Response<RegionEnrollmentSummaryDto>>(url) 
            ?? new Response<RegionEnrollmentSummaryDto>(null, 400, "Erro ao obter resumo regional.");
    }

    public async Task<Response<ICollection<RegionEnrollmentSummaryDto>>> GetRegionSummariesListAsync(GetRegionSummariesListRequest request)
    {
        var url = new StringBuilder($"v1/schoolenrollvalues/summary/list?year={request.Year}");

        if (!string.IsNullOrEmpty(request.ParentLevel)) url.Append($"&parentLevel={request.ParentLevel}");
        if (request.ParentId.HasValue) url.Append($"&parentId={request.ParentId}");

        return await _client.GetFromJsonAsync<Response<ICollection<RegionEnrollmentSummaryDto>>>(url.ToString())
               ?? new Response<ICollection<RegionEnrollmentSummaryDto>>(null, 400, "Erro ao obter lista de resumos.");
    }

    public async Task<Response<SchoolEnrollmentDetailDto>> GetSchoolEnrollmentDetailByFilterAsync(GetSchoolEnrollmentSummaryByFilterRequest request)
    {
        var url = $"v1/schoolenrollvalues/summary/{request.SchoolId}?year={request.Year}";

        return await _client.GetFromJsonAsync<Response<SchoolEnrollmentDetailDto>>(url)
               ?? new Response<SchoolEnrollmentDetailDto>(null, 400, "Erro ao obter detalhes da escola.");
    }

    public async Task<Response<ICollection<SchoolEnrollValuesStudentsDto>>> GetStudentsWithSeriesByYearAsync(GetStudentsWithSeriesByYearRequest request)
    {
        var stopwatch = Stopwatch.StartNew();

        var result = await _client.GetFromJsonAsync<Response<ICollection<SchoolEnrollValuesStudentsDto>>>(
            $"v1/schoolenrollvalues/students/{request.Year}"
        );

        stopwatch.Stop();

        Console.WriteLine($"Tempo da requisição: {stopwatch.ElapsedMilliseconds} ms");

        return result ?? new Response<ICollection<SchoolEnrollValuesStudentsDto>>(null, 400, "Não foi possível obter os schoolenrollstudents.");
    }

    public async Task<Response<ICollection<SchoolEnrollValuesTeachersDto>>> GetTeachersWithSeriesByYearAsync(GetTeachersWithSeriesByYearRequest request)
    {
        var stopwatch = Stopwatch.StartNew();

        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true 
        };

        var result = await _client.GetFromJsonAsync<Response<ICollection<SchoolEnrollValuesTeachersDto>>>(
            $"v1/schoolenrollvalues/teachers/{request.Year}",
            options
        );

        stopwatch.Stop();

        Console.WriteLine($"Tempo da requisição: {stopwatch.ElapsedMilliseconds} ms");

        return result ?? new Response<ICollection<SchoolEnrollValuesTeachersDto>>(null, 400, "Não foi possível obter os schoolenrollstudents.");
    }

    public async Task<Response<ICollection<SchoolEnrollValuesGovernanceDto>>> GetGovernanceByYearAsync(GetSchoolEnrollValuesGovernanceByYearRequest request)
    {
        var stopwatch = Stopwatch.StartNew();

        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true 
        };

        var result = await _client.GetFromJsonAsync<Response<ICollection<SchoolEnrollValuesGovernanceDto>>>(
            $"v1/schoolenrollvalues/governance/{request.Year}",
            options
        );

        stopwatch.Stop();

        Console.WriteLine($"Tempo da requisição: {stopwatch.ElapsedMilliseconds} ms");

        return result ?? new Response<ICollection<SchoolEnrollValuesGovernanceDto>>(null, 400, "Não foi possível obter os schoolenrollstudents.");
    }
}