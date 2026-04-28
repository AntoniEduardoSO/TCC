using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Arkhos.Api.Data.Mappings;

public class SchoolInfoMapping : IEntityTypeConfiguration<SchoolInfo>
{
    public void Configure(EntityTypeBuilder<SchoolInfo> builder)
    {
        builder.ToTable("school_info");

        builder.HasKey(x => new { x.IdEscola, x.Ano });

        builder.HasIndex(x => x.Ano);
        builder.HasIndex(x => x.CityInfoId);

        builder.Property(x => x.IdEscola).HasColumnName("escola_id").IsRequired();
        builder.Property(x => x.NomeEscola).HasColumnName("nome_escola").IsRequired();
        builder.Property(x => x.CityInfoId).HasColumnName("id_municipio_fk").IsRequired();
        builder.Property(x => x.Dependencia).HasColumnName("dependencia").IsRequired();
        builder.Property(x => x.Funcionamento).HasColumnName("funcionamento").IsRequired();
        builder.Property(x => x.Sede).HasColumnName("sede").IsRequired(false);
        builder.Property(x => x.Alocacao).HasColumnName("alocacao");
        builder.Property(x => x.Localizacao).HasColumnName("localizacao");
        builder.Property(x => x.Ocupacao).HasColumnName("ocupacao").IsRequired();
        builder.Property(x => x.Ano).HasColumnName("ano").IsRequired();
        builder.Property(x => x.Endereco).HasColumnName("endereco");
        builder.Property(x => x.Telefone).HasColumnName("telefone").IsRequired(false);
        builder.Property(x => x.Lat).HasColumnName("lat");
        builder.Property(x => x.Lon).HasColumnName("lon");

        builder.HasOne(x => x.CityInfo)
            .WithMany(x => x.SchoolInfos)
            .HasForeignKey(x => new { x.CityInfoId, x.Ano })
            .HasPrincipalKey(x => new { x.MunicipioId, x.Ano });
    }
}