using Arkhos.Api.Common.Api;
using Arkhos.Api.Data;
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


var app = builder.Build();

app.InitArkhosDatabase();
app.ConfigureDevEnvironment();

app.MapGet("/", () => "Hello World!");

app.Run();
