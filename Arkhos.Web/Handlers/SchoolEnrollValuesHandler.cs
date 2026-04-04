using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolEnrollValues;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Responses;

namespace Arkhos.Web.Handlers;
public class SchoolEnrollValuesHandler(IHttpClientFactory httpClientFactory) : ISchoolEnrollValuesHandler
{
    private readonly HttpClient _client = httpClientFactory.CreateClient(Configuration.HttpClientName);

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
}