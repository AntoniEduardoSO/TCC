using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Arkhos.Api.Data.Mappings;

public class CityInfoMapping : IEntityTypeConfiguration<CityInfo>
{
    public void Configure(EntityTypeBuilder<CityInfo> builder)
    {
        builder.ToTable("city_info");

        builder.HasKey(x => x.Id);

        builder.Property
        (x => x.NomeMunicipio)
        .HasColumnName("nome_municipio")
        .HasColumnType("text")
        .IsRequired();

        builder.Property
        (x => x.NomeMesorregiao)
        .HasColumnName("nome_mesorregiao")
        .HasColumnType("text")
        .IsRequired();

        builder.Property
        (x => x.IdMesorregiao)
        .HasColumnName("id_messorregiao")
        .HasColumnType("bigint")
        .IsRequired();

        builder.Property
        (x => x.NomeMicrorregiao)
        .HasColumnName("nome_microrregiao")
        .HasColumnType("text")
        .IsRequired();

        builder.Property
        (x => x.IdMesorregiao)
        .HasColumnName("id_microrregiao")
        .HasColumnType("bigint")
        .IsRequired();

        builder.HasMany(x => x.SchoolInfos)
        .WithOne(x => x.CityInfo)
        .HasForeignKey(x => x.CityInfoId)
        .IsRequired();
    }
}