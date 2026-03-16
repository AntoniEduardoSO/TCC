using System.Reflection;
using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;

namespace Arkhos.Api.Data;

public class AppDbContext(DbContextOptions options) 
    : DbContext(options)
{
    public DbSet<CityInfo> CityInfos {get;set;} = null!;

    public DbSet<SchoolInfo> SchoolInfos {get;set;} = null!;

    public DbSet<SchoolRating> SchoolRatings {get;set;} = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfigurationsFromAssembly(Assembly.GetExecutingAssembly());
    }
 
}