using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolInfraValues;
using Arkhos.Core.Requests.SchoolInfraValues;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolInfraValues;

public class GetSchoolInfraValuesPedagogicalRoomsByYearEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
    => app.MapGet("/pedagogicalrooms/{year}", HandleAsync)
    .WithName("SchoolInfraValues: Get Pedagogical Rooms By Year")
    .WithSummary("Pega as salas pedagógicas por ano.")
    .Produces<Response<ICollection<SchoolInfraValuesPedagogicalRoomsDto>>>();


    public static async Task<IResult> HandleAsync(
        int year,
        [FromServices] ISchoolInfraValuesHandler handler,
        [FromQuery] int? limit = null)
    {
        var request = new GetSchoolInfraValuesPedagogicalRoomsByYearRequest
        {
            Year = year,
            Limit = limit ?? null
        };

        var result = await handler.GetPedagogicalRoomsByYearAsync(request);

        return result.IsSuccess
            ? TypedResults.Ok(result)
            : TypedResults.BadRequest(result);
    }
}