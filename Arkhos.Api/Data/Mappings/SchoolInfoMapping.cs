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

        builder.Property(x => x.IdEscola)
            .HasColumnName("escola_id")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Property(x => x.NomeEscola)
            .HasColumnName("nome_escola")
            .HasColumnType("TEXT")
            .IsRequired();

        builder.Property(x => x.CityInfoId)
            .HasColumnName("id_municipio_fk")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Property(x => x.Dependencia)
            .HasColumnName("dependencia")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Property(x => x.Funcionamento)
            .HasColumnName("funcionamento")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Property(x => x.Sede)
            .HasColumnName("sede")
            .HasColumnType("INTEGER")
            .IsRequired(false);

        builder.Property(x => x.Alocacao)
            .HasColumnName("alocacao")
            .HasColumnType("INTEGER");

        builder.Property(x => x.Ocupacao)
            .HasColumnName("ocupacao")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Property(x => x.Ano)
            .HasColumnName("ano")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Property(x => x.Endereco)
            .HasColumnName("endereco")
            .HasColumnType("TEXT");

        builder.Property(x => x.Telefone)
            .HasColumnName("telefone")
            .HasColumnType("TEXT")
            .IsRequired(false);

        builder.Property(x => x.Lat)
            .HasColumnName("lat")
            .HasColumnType("REAL");

        builder.Property(x => x.Lon)
            .HasColumnName("lon")
            .HasColumnType("REAL");

        builder.HasOne(x => x.CityInfo)
            .WithMany(x => x.SchoolInfos)
            .HasForeignKey(x => new { x.CityInfoId, x.Ano })
            .HasPrincipalKey(x => new { x.MunicipioId, x.Ano });

    }
}