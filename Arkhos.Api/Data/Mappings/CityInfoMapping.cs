using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Arkhos.Api.Data.Mappings;

public class CityInfoMapping : IEntityTypeConfiguration<CityInfo>
{
    public void Configure(EntityTypeBuilder<CityInfo> builder)
    {
        builder.ToTable("city_info");

        builder.HasKey(x => new { x.MunicipioId, x.Ano });
        
        builder.HasIndex(x => x.Ano);

        builder.Property(x => x.Ano).HasColumnName("ano").IsRequired();
        builder.Property(x => x.MunicipioId).HasColumnName("municipio_id").IsRequired();
        builder.Property(x => x.NomeMunicipio).HasColumnName("nome_municipio").IsRequired();
        builder.Property(x => x.NomeMesorregiao).HasColumnName("nome_mesorregiao").IsRequired();
        builder.Property(x => x.IdMesorregiao).HasColumnName("id_mesorregiao").IsRequired();
        builder.Property(x => x.NomeMicrorregiao).HasColumnName("nome_microrregiao").IsRequired();
        builder.Property(x => x.IdMicrorregiao).HasColumnName("id_microrregiao").IsRequired();
        builder.Property(x => x.AreaTerritorial).HasColumnName("area_territorial").IsRequired();
        builder.Property(x => x.PopulacaoTotal).HasColumnName("populacao_total").IsRequired();
        builder.Property(x => x.DensidadeDemografica).HasColumnName("densidade_demografica").IsRequired();
    }
}