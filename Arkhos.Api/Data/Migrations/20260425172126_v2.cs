using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Arkhos.Api.Migrations
{
    /// <inheritdoc />
    public partial class v2 : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "insights",
                columns: table => new
                {
                    id = table.Column<int>(type: "INTEGER", nullable: false)
                        .Annotation("Sqlite:Autoincrement", true),
                    axis = table.Column<string>(type: "TEXT", nullable: false),
                    level = table.Column<string>(type: "TEXT", nullable: false),
                    ano = table.Column<int>(type: "INTEGER", nullable: false),
                    tipo_insight = table.Column<string>(type: "TEXT", nullable: false),
                    titulo = table.Column<string>(type: "TEXT", nullable: false),
                    valor_destaque = table.Column<string>(type: "TEXT", nullable: false),
                    descricao = table.Column<string>(type: "TEXT", nullable: false),
                    recomendacao = table.Column<string>(type: "TEXT", nullable: false),
                    valor_baseline = table.Column<double>(type: "REAL", nullable: false),
                    id_alvo = table.Column<int>(type: "INTEGER", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_insights", x => x.id);
                });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "insights");
        }
    }
}
