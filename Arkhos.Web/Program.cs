using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using Arkhos.Web;
using MudBlazor.Services;
using Arkhos.Core.Handlers;
using Arkhos.Web.Handlers;
using Arkhos.Core.Models;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

builder.Services.AddMudServices();

var isDev = builder.HostEnvironment.IsDevelopment();

var backendUrl = isDev 
    ? "http://localhost:5040" 
    : "https://arkhos-ub0p.onrender.com";

builder.Services.AddHttpClient(Configuration.HttpClientName, options =>
{
    Console.WriteLine($"Conectando na API: {backendUrl}");
    options.BaseAddress = new Uri(backendUrl!);
});

builder.Services.AddScoped<ICityInfosHandler, CityInfosHandler>();
builder.Services.AddScoped<ISchoolInfosHandler, SchoolInfoHandler>();
builder.Services.AddScoped<ISchoolEnrollValuesHandler, SchoolEnrollValuesHandler>();
builder.Services.AddScoped<ISchoolInfraValuesHandler, SchoolInfraValuesHandler>();
builder.Services.AddScoped<ISchoolRatingsHandler, SchoolRatingHandler>();
builder.Services.AddScoped<ITargetInsightsHandler, TargetInsightHandler>();
builder.Services.AddScoped<DashboardStateService>();

await builder.Build().RunAsync();
