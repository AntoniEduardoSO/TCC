using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto;
using Arkhos.Core.Requests.SchoolRatings;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolRatings;

public class GetSchoolRatingDropoutByYearEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
    => app.MapGet("/dropout/{year}", HandleAsync)
        .WithName("SchoolRatingsDrop: Get By Year")
        .WithSummary("Pega o schoolratingsdrop pelo ano.")
        .Produces<Response<ICollection<SchoolRatingDropDto>>>();

    
    public static async Task<IResult> HandleAsync(
        int year, 
        [FromServices] ISchoolRatingsHandler handler,
        [FromQuery] int? limit = null)
    {
        var request = new GetSchoolRatingDropByYearRequest
        {
            Year = year
        };

        var result = await handler.GetDropByYearAsync(request);

        return result.IsSuccess
            ? TypedResults.Ok(result)
            : TypedResults.BadRequest(result);
    }
}