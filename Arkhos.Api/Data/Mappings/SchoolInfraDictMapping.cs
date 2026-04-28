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

        builder.HasIndex(x => x.Variavel);

        builder.Property(x => x.Id).HasColumnName("id");
        builder.Property(x => x.Variavel).HasColumnName("variavel").IsRequired();
        builder.Property(x => x.Descricao).HasColumnName("descricao").IsRequired();
        builder.Property(x => x.Tipo).HasColumnName("tipo").IsRequired();
        builder.Property(x => x.Tamanho).HasColumnName("tamanho").IsRequired();
        builder.Property(x => x.Grupo).HasColumnName("grupo").IsRequired();
    }
}