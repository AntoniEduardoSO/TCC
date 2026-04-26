using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolRating;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolRatings;

public class GetSchoolRatingDetailEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
        => app.MapGet("/summary/{schoolId:int}", HandleAsync)
            .WithName("SchoolRatings: Get School Detail")
            .WithSummary("Detalhes de ratings e finanças de uma escola específica.")
            .Produces<Response<SchoolRatingDetailDto>>();

    public static async Task<IResult> HandleAsync(
        int schoolId,
        [FromQuery] int year,
        [FromServices] ISchoolRatingsHandler handler)
    {
        var result = await handler.GetSchoolDetailAsync(schoolId, year);
        return result.IsSuccess ? TypedResults.Ok(result) : TypedResults.BadRequest(result);
    }
}