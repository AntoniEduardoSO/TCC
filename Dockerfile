FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY ["Arkhos.Api/Arkhos.Api.csproj", "Arkhos.Api/"]
COPY ["Arkhos.Core/Arkhos.Core.csproj", "Arkhos.Core/"]


RUN dotnet restore "Arkhos.Api/Arkhos.Api.csproj"

COPY . .

WORKDIR "/src/Arkhos.Api"
RUN dotnet build "Arkhos.Api.csproj" -c Release -o /app/build
RUN dotnet publish "Arkhos.Api.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

RUN apt-get update && apt-get install -y python3 python3-pandas && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/publish .

COPY --from=build /src/web-scraping /app/web-scraping

EXPOSE 8080
ENV ASPNETCORE_URLS=http://+:8080

ENTRYPOINT ["dotnet", "Arkhos.Api.dll"]