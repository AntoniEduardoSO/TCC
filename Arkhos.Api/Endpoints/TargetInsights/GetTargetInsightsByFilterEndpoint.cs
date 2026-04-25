using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Responses;
using Arkhos.Core.Models;
using Microsoft.AspNetCore.Mvc;
using Arkhos.Core.Requests.TargetInsights;

namespace Arkhos.Api.Endpoints.TargetInsights;

public class GetTargetInsightsByFilterEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
    => app.MapGet("/", HandleAsync)
    .WithName("Insights: Get By Filter")
    .WithSummary("Pega os insights pelos filtos.")
    .Produces<Response<ICollection<TargetInsight>>>();

    public static async Task<IResult> HandleAsync(
        [FromServices] ITargetInsightsHandler handler,
        [FromQuery] int? limit = null,
        [FromQuery] int? year = 2024,
        [FromQuery] string? level = null,
        [FromQuery] int? target = null
        )
    {
        var request = new GetTargetInsightsByFilterRequest
        {
            Year = year,
            Limit = limit,
            Level = level,
            Target = target
        };

        var result = await handler.GetInsightsByFilterAsync(request);

        return result.IsSuccess
            ? TypedResults.Ok(result)
            : TypedResults.BadRequest(result);
    }
}