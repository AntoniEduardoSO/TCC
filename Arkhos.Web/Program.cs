using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using Arkhos.Web;
using MudBlazor.Services;
using Arkhos.Core.Handlers;
using Arkhos.Web.Handlers;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

builder.Services.AddMudServices();

var backendUrl = builder.Configuration.GetValue<string>("BackendUrl") 
                 ?? "https://arkhos-ub0p.onrender.com";

Console.WriteLine($"[LOG BLAZOR] Inicializando Frontend.");
Console.WriteLine($"[LOG BLAZOR] A API alvo é: {backendUrl}");

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
builder.Services.AddScoped<DashboardStateService>();


await builder.Build().RunAsync();
