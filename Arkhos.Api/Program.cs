using Arkhos.Api.Common.Api;
using Arkhos.Api.Data;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models;
using Arkhos.Core.Requests.SchoolInfos;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

var cnnStr = builder
    .Configuration
    .GetConnectionString("DefaultConnection") ?? string.Empty;

builder.Services.AddDbContext<AppDbContext>(
    x =>
    {
        x.UseNpgsql(cnnStr);
    }
);

builder.AddDocumentation();
builder.AddServices();



var app = builder.Build();

app.InitArkhosDatabase();
app.ConfigureDevEnvironment();

app.MapGet("v1/schoolinfos/{year}", 
    async (long year, 
    [FromServices] ISchoolInfosHandler handler)
    =>
    {
        var request = new GetSchoolInfoByYearRequest { Year = year };
        return await handler.GetByYearAsync(request);
    })
    .WithName("SchoolInfos: Get By Year")
    .WithSummary("Pega o schoolinfos pelo ano.")
    .Produces<Response<ICollection<SchoolInfo>>>();

app.Run();
