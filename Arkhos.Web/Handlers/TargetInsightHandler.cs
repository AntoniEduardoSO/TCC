using System.Net.Http.Json;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models;
using Arkhos.Core.Requests.TargetInsights;
using Arkhos.Core.Responses;

namespace Arkhos.Web.Handlers;
public class TargetInsightHandler(IHttpClientFactory httpClientFactory) : ITargetInsightsHandler
{
    private readonly HttpClient _client = httpClientFactory.CreateClient(Configuration.HttpClientName);

    public async Task<Response<ICollection<TargetInsight>>> GetInsightsByFilterAsync(GetTargetInsightsByFilterRequest request)
    {
        var url = "v1/insights";

        var queryParams = new List<string>();

        if (request.Year.HasValue)
            queryParams.Add($"year={request.Year}");

        if (!string.IsNullOrWhiteSpace(request.Level))
            queryParams.Add($"level={request.Level}");

        if (request.Target.HasValue)
            queryParams.Add($"target={request.Target}");

        if (request.Limit.HasValue)
            queryParams.Add($"limit={request.Limit}");

        if (queryParams.Count != 0)
        {
            url += "?" + string.Join("&", queryParams);
        }

        var result = await _client.GetFromJsonAsync<Response<ICollection<TargetInsight>>>(url);

        return result ?? new Response<ICollection<TargetInsight>>(null, 400, "Não foi possível obter o insight.");
    }
}