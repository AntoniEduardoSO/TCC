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

COPY --from=build /app/publish .

EXPOSE 8080
ENV ASPNETCORE_URLS=http://+:8080

ENTRYPOINT ["dotnet", "Arkhos.Api.dll"]