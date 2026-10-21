FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/DispatchLedger.Api/DispatchLedger.Api.csproj src/DispatchLedger.Api/
RUN dotnet restore src/DispatchLedger.Api/DispatchLedger.Api.csproj
COPY src/DispatchLedger.Api/ src/DispatchLedger.Api/
RUN dotnet publish src/DispatchLedger.Api/DispatchLedger.Api.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "DispatchLedger.Api.dll"]
