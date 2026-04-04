using System.Diagnostics;
using System.Net.Http.Json;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolInfo;
using Arkhos.Core.Requests.SchoolInfos;
using Arkhos.Core.Responses;

namespace Arkhos.Web.Handlers;

public class SchoolInfoHandler(IHttpClientFactory httpClientFactory) : ISchoolInfosHandler
{
    private readonly HttpClient _client = httpClientFactory.CreateClient(Configuration.HttpClientName);

    public async Task<Response<ICollection<SchoolInfoMapDto>>> GetByYearAsync(GetSchoolInfoByYearRequest request)
    {
        var stopwatch = Stopwatch.StartNew();

        var result = await _client.GetFromJsonAsync<Response<ICollection<SchoolInfoMapDto>>>(
            $"v1/schoolinfo/{request.Year}"
        );

        stopwatch.Stop();

        Console.WriteLine($"Tempo da requisição: {stopwatch.ElapsedMilliseconds} ms");

        return result ?? new Response<ICollection<SchoolInfoMapDto>>(null, 400, "Não foi possível obter os schoolinfos.");
    }
}