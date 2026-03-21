using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Arkhos.Api.Data.Mappings;

public class SchoolInfoMapping : IEntityTypeConfiguration<SchoolInfo>
{
    public void Configure(EntityTypeBuilder<SchoolInfo> builder)
    {
        builder.ToTable("school_info");

        builder.HasKey(x => x.Id);

        builder.HasIndex(x => new { x.IdEscola, x.Ano });
        builder.HasIndex(x => x.Ano);

        builder.Property(x => x.IdEscola)
        .HasColumnType("int")
        .HasColumnName("escola_id")
        .IsRequired();

        builder.Property(x => x.NomeEscola)
        .HasColumnType("text")
        .HasColumnName("nome_escola")
        .IsRequired();

        builder.Property(x => x.CityInfoId)
        .HasColumnName("id_municipio_fk")
        .HasColumnType("int")
        .IsRequired();

        builder.Property(x => x.Dependencia)
        .HasColumnType("smallint")
        .HasColumnName("dependencia")
        .IsRequired();


        builder.Property(x => x.Funcionamento)
        .HasColumnType("smallint")
        .HasColumnName("funcionamento")
        .IsRequired();

        builder.Property(x => x.Sede)
        .HasColumnType("int")
        .HasColumnName("sede")
        .IsRequired(false);

        builder.Property(x => x.Alocacao)
        .HasColumnType("smallint")
        .HasColumnName("alocacao");

        builder.Property(x => x.Ocupacao)
        .HasColumnType("smallint")
        .HasColumnName("ocupacao")
        .IsRequired();

        builder.Property(x => x.Ano)
        .HasColumnType("int")
        .HasColumnName("ano")
        .IsRequired();

        builder.Property(x => x.Endereco)
        .HasColumnType("text")
        .HasColumnName("endereco")
        .IsRequired();

        builder.Property(x => x.Telefone)
        .HasColumnType("text")
        .HasColumnName("telefone")
        .IsRequired();

        builder.Property(x => x.Lat)
        .HasColumnType("numeric(9,6)")
        .HasColumnName("lat");

        builder.Property(x => x.Lon)
        .HasColumnType("numeric(9,6)")
        .HasColumnName("lon");

        builder.HasOne(x => x.CityInfo)
       .WithMany(x => x.SchoolInfos)
       .HasForeignKey(x => new { x.CityInfoId, x.Ano })
       .HasPrincipalKey(x => new { x.MunicipioId, x.Ano });

    }
}