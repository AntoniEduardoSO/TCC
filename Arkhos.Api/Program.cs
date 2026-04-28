using Arkhos.Api;
using Arkhos.Api.Common.Api;
using Arkhos.Api.Endpoints;
using Arkhos.Api.Services;
using Microsoft.AspNetCore.ResponseCompression;

var builder = WebApplication.CreateBuilder(args);

builder.AddConfiguration();
builder.AddDataContexts();
builder.AddCrossOrigin();
builder.AddDocumentation();
builder.AddServices();
builder.Services.AddResponseCompression(options => {
    options.EnableForHttps = true;
    options.Providers.Add<BrotliCompressionProvider>();
});
builder.Services.AddMemoryCache();
builder.Services.AddHostedService<CacheWarmupService>();

var app = builder.Build();


app.InitArkhosDatabase();
app.ConfigureDevEnvironment();
app.UseResponseCompression();
app.UseCors(ApiConfiguration.CorsPolicyName);
app.MapEndpoints();


app.Run();
