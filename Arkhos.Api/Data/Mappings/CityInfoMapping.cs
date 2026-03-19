using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Arkhos.Api.Data.Mappings;

public class CityInfoMapping : IEntityTypeConfiguration<CityInfo>
{
    public void Configure(EntityTypeBuilder<CityInfo> builder)
    {
        builder.ToTable("city_info");

        builder.HasAlternateKey(x => new { x.MunicipioId, x.Ano });

        builder.Property(x => x.Ano)
        .HasColumnName("ano")
        .HasColumnType("bigint")
        .IsRequired();

        builder.Property(x => x.MunicipioId)
       .HasColumnName("municipio_id")
       .HasColumnType("bigint") 
       .IsRequired();

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
        .HasColumnName("id_mesorregiao")
        .HasColumnType("bigint")
        .IsRequired();

        builder.Property
        (x => x.NomeMicrorregiao)
        .HasColumnName("nome_microrregiao")
        .HasColumnType("text")
        .IsRequired();

        builder.Property
        (x => x.IdMicrorregiao)
        .HasColumnName("id_microrregiao")
        .HasColumnType("bigint")
        .IsRequired();

        builder.Property
        (x => x.AreaTerritorial)
        .HasColumnName("area_territorial")
        .HasColumnType("bigint")
        .IsRequired();

        builder.Property
        (x => x.PopulacaoTotal)
        .HasColumnName("populacao_total")
        .HasColumnType("bigint")
        .IsRequired();

        builder.Property
        (x => x.DensidadeDemografica)
        .HasColumnName("densidade_demografica")
        .HasColumnType("numeric(5,2)")
        .IsRequired();

        builder.HasMany(x => x.SchoolInfos)
        .WithOne(x => x.CityInfo)
        // Colunas na tabela de Escola:
        .HasForeignKey(x => new { x.CityInfoId, x.Ano }) 
        // Colunas na tabela de Cidade:
        .HasPrincipalKey(x => new { x.MunicipioId, x.Ano }) 
        .IsRequired();
    }
}