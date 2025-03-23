using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace VectorTilesASPNET_Test.Controllers
{
    [ApiController]
    [Route("api/map")]
    public class MapTilesController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<MapTilesController> _logger;
        private static List<object> _cachedSchemas;

        public MapTilesController(IConfiguration configuration, ILogger<MapTilesController> logger)
        {
            _configuration = configuration;
            _logger = logger;

            if (_cachedSchemas == null)
            {
                _cachedSchemas = LoadSchemasFromDatabase();
                _logger.LogInformation("✅ Cached Schemas: {@_cachedSchemas}", _cachedSchemas); // ✅ Log fetched schemas
            }
        }

        private List<object> LoadSchemasFromDatabase()
        {
            string connectionString = _configuration.GetConnectionString("PostgresConnection");
            var schemas = new List<object>();

            try
            {
                using var conn = new NpgsqlConnection(connectionString);
                conn.Open();

                string sqlQuery = "SELECT schema_name, min_zoom, max_zoom FROM public.available_schemas_with_zoom";

                using var cmd = new NpgsqlCommand(sqlQuery, conn);
                using var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    schemas.Add(new
                    {
                        schema_name = reader.GetString(0),
                        minZoom = reader.GetDouble(1),
                        maxZoom = reader.GetDouble(2)
                    });
                }

                _logger.LogInformation("✅ Database Response: {@schemas}", schemas);
            }
            catch (Exception ex)
            {
                _logger.LogError("Error loading schemas from database: {Message}", ex.Message);
            }

            return schemas;
        }

        [HttpGet("schemas")]
        public IActionResult GetAvailableSchemas()
        {
            return Ok(_cachedSchemas);
        }
        
        [HttpGet("layers/{schema}")]
        public IActionResult GetLayersForSchema(string schema)
        {
            string connectionString = _configuration.GetConnectionString("PostgresConnection");

            try
            {
                using var conn = new NpgsqlConnection(connectionString);
                conn.Open();

                string sqlQuery = "SELECT f_table_name FROM public.geometry_columns WHERE f_table_schema = @schema";

                using var cmd = new NpgsqlCommand(sqlQuery, conn);
                cmd.Parameters.AddWithValue("@schema", schema);
                using var reader = cmd.ExecuteReader();

                var layers = new List<object>();
                while (reader.Read())
                {
                    layers.Add(new { name = reader.GetString(0) });
                }

                return Ok(layers);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { error = "Database error", details = ex.Message });
            }
        }
    }
}
