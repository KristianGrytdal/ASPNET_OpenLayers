using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;

namespace VectorTilesASPNET_Test.Controllers
{
    [ApiController]
    [Route("api/map")]
    public class MapTilesController : ControllerBase
    {
        private readonly IConfiguration _configuration;

        public MapTilesController(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        /// <summary>
        /// Returns a list of all available vector tile layers from PostgreSQL/PostGIS.
        /// </summary>
        [HttpGet("layers")]
        public IActionResult GetAvailableLayers()
        {
            string connectionString = _configuration.GetConnectionString("PostgresConnection");
            string tileServerHost = _configuration["TileServerHost"] ?? "http://localhost:7800"; // Default to Martin Tile Server

            try
            {
                using var conn = new NpgsqlConnection(connectionString);
                conn.Open();

                string sqlQuery = "SELECT f_table_schema, f_table_name FROM public.geometry_columns";

                using var cmd = new NpgsqlCommand(sqlQuery, conn);
                using var reader = cmd.ExecuteReader();
                var layers = new List<object>();

                while (reader.Read())
                {
                    string schema = reader.GetString(0);
                    string table = reader.GetString(1);
                    string layerId = $"{schema}.{table}";
                    string tileUrl = $"{tileServerHost}/{schema}.{table}/{{z}}/{{x}}/{{y}}.pbf";

                    layers.Add(new { id = layerId, schema, table, tileUrl });
                }

                return Ok(layers);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { error = "Database error", details = ex.Message });
            }
        }

        /// <summary>
        /// Returns the tile URL for a given layer, fetched dynamically from the database.
        /// </summary>
        [HttpGet("tiles/{layerName}")]
        public IActionResult GetTileUrl(string layerName)
        {
            string connectionString = _configuration.GetConnectionString("PostgresConnection");
            string tileServerHost = _configuration["TileServerHost"] ?? "http://localhost:7800";

            try
            {
                using var conn = new NpgsqlConnection(connectionString);
                conn.Open();

                string sqlQuery = @"
                    SELECT f_table_schema, f_table_name 
                    FROM public.geometry_columns
                    WHERE CONCAT(f_table_schema, '.', f_table_name) = @layerName";

                using var cmd = new NpgsqlCommand(sqlQuery, conn);
                cmd.Parameters.AddWithValue("@layerName", layerName);
                using var reader = cmd.ExecuteReader();

                if (reader.Read())
                {
                    string schema = reader.GetString(0);
                    string table = reader.GetString(1);
                    string tileUrl = $"{tileServerHost}/{schema}.{table}/{{z}}/{{x}}/{{y}}.pbf";
                    
                    return Ok(new { layer = layerName, tileUrl });
                }

                return NotFound(new { error = "Layer not found" });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { error = "Database error", details = ex.Message });
            }
        }
    }
}
