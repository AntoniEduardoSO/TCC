using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolEnrollValues;
using Arkhos.Core.Models.Dto.SchoolInfraValues;
using Arkhos.Core.Requests.SchoolInfraValues;
using Arkhos.Core.Responses;

namespace Arkhos.Web.Handlers;

public class SchoolInfraValuesHandler(IHttpClientFactory httpClientFactory) : ISchoolInfraValuesHandler
{
    private readonly HttpClient _client = httpClientFactory.CreateClient(Configuration.HttpClientName);
    public async Task<Response<ICollection<SchoolInfraValuesPedagogicalRoomsDto>>> GetPedagogicalRoomsByYearAsync(GetSchoolInfraValuesPedagogicalRoomsByYearRequest request)
    {
        var stopwatch = Stopwatch.StartNew();

        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        var result = await _client.GetFromJsonAsync<Response<ICollection<SchoolInfraValuesPedagogicalRoomsDto>>>(
            $"v1/schoolinfravalues/pedagogicalrooms/{request.Year}",
            options
        );

        stopwatch.Stop();

        Console.WriteLine($"Tempo da requisição: {stopwatch.ElapsedMilliseconds} ms");

        return result ?? new Response<ICollection<SchoolInfraValuesPedagogicalRoomsDto>>(null, 400, "Não foi possível obter os schoolinfrapedagogicalrooms.");
    }

    public async Task<Response<ICollection<SchoolInfraValuesWellbeingDto>>> GetWellbeingByYearAsync(GetSchoolInfraValuesWellbeingByYearRequest request)
    {
       var stopwatch = Stopwatch.StartNew();

        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        var result = await _client.GetFromJsonAsync<Response<ICollection<SchoolInfraValuesWellbeingDto>>>(
            $"v1/schoolinfravalues/wellbeing/{request.Year}",
            options
        );

        stopwatch.Stop();

        Console.WriteLine($"Tempo da requisição: {stopwatch.ElapsedMilliseconds} ms");

        return result ?? new Response<ICollection<SchoolInfraValuesWellbeingDto>>(null, 400, "Não foi possível obter os schoolinfrawellbeing.");
    }
}