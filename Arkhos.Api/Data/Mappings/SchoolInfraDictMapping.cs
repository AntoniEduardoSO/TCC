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
            .HasColumnName("id")
            .HasColumnType("INTEGER");

        builder.Property(x => x.Variavel)
            .HasColumnName("variavel")
            .HasColumnType("TEXT")
            .IsRequired();

        builder.Property(x => x.Descricao)
            .HasColumnName("descricao")
            .HasColumnType("TEXT")
            .IsRequired();

        builder.Property(x => x.Tipo)
            .HasColumnName("tipo")
            .HasColumnType("TEXT")
            .IsRequired();

        builder.Property(x => x.Tamanho)
            .HasColumnName("tamanho")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Property(x => x.Grupo)
            .HasColumnName("grupo")
            .HasColumnType("TEXT")
            .IsRequired();
    }
}