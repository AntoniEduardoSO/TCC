using Arkhos.Api.Common.Api;
using Arkhos.Api.Endpoints;

var builder = WebApplication.CreateBuilder(args);

builder.AddDocumentation();
builder.AddServices();

var app = builder.Build();

app.InitArkhosDatabase();
app.ConfigureDevEnvironment();
app.MapEndpoints();

app.Run();
