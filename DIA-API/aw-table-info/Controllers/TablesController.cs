using aw_table_info.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;

namespace aw_table_info.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TablesController : ControllerBase
    {
        private readonly IConfiguration _configuration;

        public TablesController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [HttpGet]
        public async Task<IEnumerable<TableInfo>> Get()
        {
            var tables = new List<TableInfo>();
            var connectionString = _configuration.GetConnectionString("DefaultConnection");

            using (var connection = new SqlConnection(connectionString))
            {
                var command = new SqlCommand(
                    "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", connection);
                await connection.OpenAsync();
                var reader = await command.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    tables.Add(new TableInfo { TableName = reader.GetString(0) });
                }
            }

            return tables;
        }

        [HttpGet("with-rowcount")]
        public async Task<IEnumerable<TableInfo>> GetWithRowCounts()
        {
            var tables = new List<TableInfo>();
            var connectionString = _configuration.GetConnectionString("DefaultConnection");

            using (var connection = new SqlConnection(connectionString))
            {
                var command = new SqlCommand(@"
            SELECT 
                t.NAME AS Table_Name,
                SUM(p.rows) AS Total_Rows
            FROM 
                sys.tables t
            INNER JOIN      
                sys.indexes i ON t.OBJECT_ID = i.object_id
            INNER JOIN 
                sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
            WHERE 
                i.index_id <= 1
            GROUP BY 
                t.Name
            ORDER BY 
                Total_Rows DESC;", connection);

                await connection.OpenAsync();
                var reader = await command.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    tables.Add(new TableInfo
                    {
                        TableName = reader.GetString(0),
                        TotalRows = reader.GetInt64(1)
                    });
                }
            }

            return tables;
        }
    }
}
