using Arkhos.Api;
using Arkhos.Api.Common.Api;
using Arkhos.Api.Endpoints;

var builder = WebApplication.CreateBuilder(args);

builder.AddConfiguration();
builder.AddDataContexts();
builder.AddCrossOrigin();
builder.AddDocumentation();
builder.AddServices();

var app = builder.Build();

app.InitArkhosDatabase();
app.ConfigureDevEnvironment();
app.UseCors(ApiConfiguration.CorsPolicyName);
app.MapEndpoints();

app.Run();
