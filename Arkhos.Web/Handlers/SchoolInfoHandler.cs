using System.Diagnostics;
using System.Net.Http.Json;
using System.Text;
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
        var url = new StringBuilder($"v1/schoolinfo/{request.Year}");

        var queryParams = new List<string>();
        if (request.Dependencia.HasValue) queryParams.Add($"dependencia={request.Dependencia}");
        if (request.Limit.HasValue) queryParams.Add($"limit={request.Limit}");

        if (queryParams.Any())
        {
            url.Append("?");
            url.Append(string.Join("&", queryParams));
        }

        return await _client.GetFromJsonAsync<Response<ICollection<SchoolInfoMapDto>>>(url.ToString())
               ?? new Response<ICollection<SchoolInfoMapDto>>(null, 400, "Erro ao obter markers.");
    }
}