using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text.Json;
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
        private static Dictionary<double, Dictionary<string, object>> _filteredCatalogCache = new();

        public MapTilesController(IConfiguration configuration, ILogger<MapTilesController> logger)
        {
            _configuration = configuration;
            _logger = logger;

            if (_cachedSchemas == null)
            {
                _cachedSchemas = LoadSchemasFromDatabase();
                _logger.LogInformation("Cached Schemas: {@_cachedSchemas}", _cachedSchemas);
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

                string sqlQuery = @"
            SELECT schema_name, min_zoom, max_zoom, prefetch_priority 
            FROM public.available_schemas_with_zoom
            ORDER BY prefetch_priority DESC";

                using var cmd = new NpgsqlCommand(sqlQuery, conn);
                using var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    schemas.Add(new
                    {
                        schema_name = reader.GetString(0),
                        minZoom = reader.GetDouble(1),
                        maxZoom = reader.GetDouble(2),
                        prefetchPriority = reader.GetInt32(3)
                    });
                }

                _logger.LogInformation("Database Response with priority: {@schemas}", schemas);
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

        [HttpGet("catalog")]
        public async Task<IActionResult> GetFilteredCatalog([FromQuery] double zoom)
        {
            try
            {
                if (_cachedSchemas == null)
                {
                    _cachedSchemas = LoadSchemasFromDatabase();
                }

                // Round to 3 decimals for a stable cache key
                double roundedZoom = Math.Round(zoom, 3);

                var zoomsToPrewarm = new[] { zoom, zoom + 1, zoom - 1 }
                    .Where(z => z >= 0 && z <= 22)
                    .Select(z => Math.Round(z, 3))
                    .Distinct();

                foreach (var z in zoomsToPrewarm)
                {
                    if (!_filteredCatalogCache.ContainsKey(z))
                    {
                        _logger.LogInformation($"⏳ Building catalog for zoom {z}...");
                        _filteredCatalogCache[z] = await BuildCatalogForZoom(z); // pass exact zoom
                    }
                }

                if (_filteredCatalogCache.TryGetValue(roundedZoom, out var result))
                {
                    _logger.LogInformation("✅ Returning cached catalog for zoom {Zoom}", roundedZoom);
                    return Ok(new { tiles = result });
                }
                else
                {
                    _logger.LogWarning("⚠️ No cached catalog found for zoom {Zoom}", roundedZoom);
                    return Ok(new { tiles = new Dictionary<string, object>() });
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "🔥 Error in GetFilteredCatalog");
                return StatusCode(500, new { error = "Internal Server Error", details = ex.Message });
            }
        }

        private async Task<Dictionary<string, object>> BuildCatalogForZoom(double zoom)
        {
            var validSchemas = _cachedSchemas
                .Where(s => zoom >= (double)s.GetType().GetProperty("minZoom").GetValue(s)
                            && zoom <= (double)s.GetType().GetProperty("maxZoom").GetValue(s))
                .Select(s => s.GetType().GetProperty("schema_name").GetValue(s)?.ToString())
                .Where(name => !string.IsNullOrEmpty(name))
                .ToHashSet();

            using var httpClient = new HttpClient();
            var catalogRes = await httpClient.GetStringAsync("http://localhost:7800/catalog");
            var catalogJson = JsonDocument.Parse(catalogRes);

            var simplifiedCatalog = new Dictionary<string, object>();

            foreach (var tile in catalogJson.RootElement.GetProperty("tiles").EnumerateObject())
            {
                var tileKey = tile.Name; // e.g., "elv.5", "dyrketmark.2"
                var description = tile.Value.GetProperty("description").GetString();

                if (string.IsNullOrEmpty(description)) continue;

                var parts = description.Split('.');
                if (parts.Length < 2) continue;

                var schema = parts[0];
                var table = parts[1];

                if (validSchemas.Contains(schema) || schema == "public")
                {
                    if (!simplifiedCatalog.ContainsKey(table))
                    {
                        simplifiedCatalog[table] = new
                        {
                            schema,
                            tileKey,
                            url = $"/{tileKey}/{{z}}/{{x}}/{{y}}"
                        };
                    }
                }
            }

            return simplifiedCatalog;
        }
    }
}