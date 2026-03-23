using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Arkhos.Api.Data.Mappings;

public class SchoolInfraDictMapping : IEntityTypeConfiguration<SchoolInfraDict>
{
    public void Configure(EntityTypeBuilder<SchoolInfraDict> builder)
    {
        builder.ToTable("school_infra_dict");

        builder.HasKey(x => x.Id);

        builder.Property(x => x.Id)
        .HasColumnName("id");

        builder.Property(x => x.Variavel)
        .HasColumnType("text")
        .HasColumnName("variavel")
        .IsRequired();

        builder.Property(x => x.Descricao)
        .HasColumnType("text")
        .HasColumnName("descricao")
        .IsRequired();

        builder.Property(x => x.Tipo)
        .HasColumnType("text")
        .HasColumnName("tipo")
        .IsRequired();

        builder.Property(x => x.Tamanho)
        .HasColumnType("text")
        .HasColumnName("tamanho")
        .IsRequired();

        builder.Property(x => x.Grupo)
        .HasColumnType("text")
        .HasColumnName("grupo")
        .IsRequired();
    }
}