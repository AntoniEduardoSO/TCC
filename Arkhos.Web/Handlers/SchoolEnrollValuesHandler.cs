using System.Diagnostics;
using System.Net.Http.Json;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Responses;

namespace Arkhos.Web.Handlers;
public class SchoolEnrollValuesHandler(IHttpClientFactory httpClientFactory) : ISchoolEnrollValuesHandler
{
    private readonly HttpClient _client = httpClientFactory.CreateClient(Configuration.HttpClientName);

    public async Task<Response<ICollection<SchoolEnrollValuesStudentsDto>>> GetStudentsWithSeriesByYearAsync(GetStudentsWithSeriesByYearRequest request)
    {
        var stopwatch = Stopwatch.StartNew();

        var result = await _client.GetFromJsonAsync<Response<ICollection<SchoolEnrollValuesStudentsDto>>>(
            $"v1/schoolenrollvalues/{request.Year}"
        );

        stopwatch.Stop();

        Console.WriteLine($"Tempo da requisição: {stopwatch.ElapsedMilliseconds} ms");

        return result ?? new Response<ICollection<SchoolEnrollValuesStudentsDto>>(null, 400, "Não foi possível obter os schoolenrollstudents.");
    }
}