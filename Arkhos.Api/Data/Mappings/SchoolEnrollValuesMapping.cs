namespace Arkhos.Api.Data.Mappings;

using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

public class SchoolEnrollValuesMapping : IEntityTypeConfiguration<SchoolEnrollValues>
{
    public void Configure(EntityTypeBuilder<SchoolEnrollValues> builder)
    {
        builder.ToTable("school_enroll_values");

        builder.HasKey(x => new { x.Ano, x.IdEscolaEnrollValues, x.AtributoId });

        builder.Property(x => x.Ano)
            .HasColumnName("ano")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Property(x => x.IdEscolaEnrollValues)
            .HasColumnName("id_escola_fk")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Property(x => x.AtributoId)
            .HasColumnName("id_atributo")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Property(x => x.TipoAtributo)
            .HasColumnName("tipo_atributo")
            .HasColumnType("TEXT")
            .IsRequired();

        builder.Property(x => x.Valor)
            .HasColumnName("valor")
            .HasColumnType("REAL")
            .IsRequired();

        builder.HasOne(x => x.SchoolEnrollDict)
            .WithMany(x => x.SchoolEnrollValues)
            .HasForeignKey(x => x.AtributoId)
            .HasPrincipalKey(x => x.Id);

        builder.HasOne(x => x.SchoolInfo)
            .WithMany(x => x.SchoolEnrollValues)
            .HasForeignKey(x => new { x.IdEscolaEnrollValues, x.Ano })
            .HasPrincipalKey(x => new { x.IdEscola, x.Ano });
    }
}