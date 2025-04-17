// This version assumes you're using pg_tileserv instead of Martin
// It parses index.json instead of /catalog, and builds a simplified catalog for the frontend

using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;

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
            }
        }

        private List<object> LoadSchemasFromDatabase()
        {
            var list = new List<object>();
            var connStr = _configuration.GetConnectionString("PostgresConnection");

            try
            {
                using var conn = new NpgsqlConnection(connStr);
                conn.Open();

                string sql = "SELECT schema_name, min_zoom, max_zoom, prefetch_priority FROM public.available_schemas_with_zoom ORDER BY prefetch_priority DESC";

                using var cmd = new NpgsqlCommand(sql, conn);
                using var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    list.Add(new
                    {
                        schema_name = reader.GetString(0),
                        minZoom = reader.GetDouble(1),
                        maxZoom = reader.GetDouble(2),
                        prefetchPriority = reader.GetInt32(3)
                    });
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load schema zoom metadata");
            }

            return list;
        }

        [HttpGet("schemas")]
        public IActionResult GetSchemas() => Ok(_cachedSchemas);

        [HttpGet("layers/{schema}")]
        public IActionResult GetLayers(string schema)
        {
            var connStr = _configuration.GetConnectionString("PostgresConnection");
            var list = new List<object>();

            try
            {
                using var conn = new NpgsqlConnection(connStr);
                conn.Open();

                var cmd = new NpgsqlCommand("SELECT f_table_name FROM public.geometry_columns WHERE f_table_schema = @schema", conn);
                cmd.Parameters.AddWithValue("@schema", schema);

                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    list.Add(new { name = reader.GetString(0) });
                }

                return Ok(list);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to fetch layers for schema: {Schema}", schema);
                return StatusCode(500);
            }
        }

        [HttpGet("catalog")]
        public async Task<IActionResult> GetFilteredCatalog([FromQuery] double zoom, [FromQuery] bool triggeredByToggle = false)
        {
            try
            {
                double z = Math.Round(zoom, 3);
                if (!_filteredCatalogCache.TryGetValue(z, out var cached))
                {
                    cached = await BuildCatalogForZoom(z);
                    _filteredCatalogCache[z] = cached;
                }

                if (triggeredByToggle)
                {
                    _ = Task.Run(async () =>
                    {
                        foreach (var near in new[] { z - 1, z + 1 })
                        {
                            double zNear = Math.Round(near, 3);
                            if (!_filteredCatalogCache.ContainsKey(zNear))
                            {
                                var built = await BuildCatalogForZoom(zNear);
                                _filteredCatalogCache[zNear] = built;
                            }
                        }
                    });
                }

                return Ok(new { tiles = cached });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error building pg_tileserv catalog");
                return StatusCode(500);
            }
        }

        private async Task<Dictionary<string, object>> BuildCatalogForZoom(double zoom)
        {
            var validSchemas = _cachedSchemas
                .Where(s =>
                    zoom >= (double)s.GetType().GetProperty("minZoom").GetValue(s)
                    && zoom <= (double)s.GetType().GetProperty("maxZoom").GetValue(s))
                .Select(s => s.GetType().GetProperty("schema_name").GetValue(s)?.ToString())
                .ToHashSet();

            var result = new Dictionary<string, object>();

            using var client = new HttpClient();
            var raw = await client.GetStringAsync("http://localhost:7800/index.json");
            var json = JsonDocument.Parse(raw);

            foreach (var entry in json.RootElement.EnumerateObject())
            {
                var fullKey = entry.Name;
                if (!fullKey.Contains('.')) continue;

                var parts = fullKey.Split('.');
                var schema = parts[0];
                var table = parts[1];

                if (!validSchemas.Contains(schema) && schema != "public") continue;

                result[$"{schema}.{table}"] = new
                {
                    schema,
                    table,
                    url = $"http://localhost:7800/{fullKey}/{{z}}/{{x}}/{{y}}.pbf"
                };
            }

            return result;
        }
    }
}
