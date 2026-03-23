namespace Arkhos.Api.Data.Mappings;

using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

public class SchoolEnrollValuesMapping : IEntityTypeConfiguration<SchoolEnrollValues>
{
    public void Configure(EntityTypeBuilder<SchoolEnrollValues> builder)
    {
        builder.ToTable("school_enroll_values");

        builder.HasKey(x => x.Id);

        builder.Property(x => x.IdEscolaEnrollValues)
        .HasColumnName("id_escola_fk")
        .HasColumnType("int")
        .IsRequired();

        builder.Property(x => x.Ano)
        .HasColumnName("ano")
        .HasColumnType("int")
        .IsRequired();

        builder.Property(x => x.TipoAtributo)
        .HasColumnType("text")
        .HasColumnName("tipo_atributo")
        .IsRequired();

        builder.Property(x => x.Valor)
        .HasColumnType("numeric(10,1)")
        .HasColumnName("valor")
        .IsRequired();

        builder.HasOne(x => x.SchoolEnrollDict)
       .WithMany(x => x.SchoolEnrollValues)
       .HasForeignKey(x => x.AtributoId)
       .HasPrincipalKey(x => x.Id);

       builder.Property(x => x.AtributoId)
       .HasColumnName("id_atributo")
       .HasColumnType("int")
       .IsRequired();



       builder.HasOne(x => x.SchoolInfo)
       .WithMany(x => x.SchoolEnrollValues)
       .HasForeignKey(x => new { x.IdEscolaEnrollValues, x.Ano })
       .HasPrincipalKey(x => new { x.IdEscola, x.Ano });
    }
}