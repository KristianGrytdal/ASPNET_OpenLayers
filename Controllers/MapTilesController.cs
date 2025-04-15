﻿using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Linq;
using System.Net.Http;
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
                _logger.LogInformation("Cached Schemas: {@Schemas}", _cachedSchemas);
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
        public async Task<IActionResult> GetFilteredCatalog([FromQuery] double zoom, [FromQuery] bool triggeredByToggle = false)

        {
            try
            {
                if (_cachedSchemas == null)
                {
                    _cachedSchemas = LoadSchemasFromDatabase();
                }

                double roundedZoom = Math.Round(zoom, 3);

                // Step 1: Build for requested zoom immediately
                if (!_filteredCatalogCache.TryGetValue(roundedZoom, out var catalog))
                {
                    _logger.LogInformation("⏳ Building catalog for zoom {Zoom}...", roundedZoom);
                    catalog = await BuildCatalogForZoom(roundedZoom);
                    _filteredCatalogCache[roundedZoom] = catalog;
                }
                else
                {
                    _logger.LogInformation("Returning cached catalog for zoom {Zoom}", roundedZoom);
                }

                // Only start background fetching if triggered by toggle
                if (triggeredByToggle)
                {
                    _ = Task.Run(async () =>
                    {
                        foreach (var z in new[] { roundedZoom - 1, roundedZoom + 1 })
                        {
                            if (z >= 0 && z <= 22)
                            {
                                double zKey = Math.Round(z, 1);
                                if (!_filteredCatalogCache.ContainsKey(zKey))
                                {
                                    _logger.LogInformation("(Async) Building catalog for nearby zoom {Zoom}...", zKey);
                                    var cat = await BuildCatalogForZoom(zKey);
                                    _filteredCatalogCache[zKey] = cat;
                                }
                            }
                        }
                    });
                }
                
                _logger.LogInformation("Catalog for zoom {Zoom} includes: {Keys}", roundedZoom, string.Join(", ", catalog.Keys));
                return Ok(new { tiles = catalog });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in GetFilteredCatalog");
                return StatusCode(500, new { error = "Internal Server Error", details = ex.Message });
            }
        }


        private async Task<Dictionary<string, object>> BuildCatalogForZoom(double zoom)
        {
            var validSchemas = _cachedSchemas
                .Where(s =>
                    zoom >= (double)s.GetType().GetProperty("minZoom").GetValue(s)
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
                var tileKey = tile.Name;

                if (!tile.Value.TryGetProperty("description", out var descElement)) continue;
                var description = descElement.GetString();
                if (string.IsNullOrEmpty(description)) continue;

                var parts = description.Split('.');
                if (parts.Length < 2) continue;

                var schema = parts[0];
                var table = parts[1];

                // Match only relevant schemas
                if (!validSchemas.Contains(schema) && schema != "public") continue;

                var fullKey = $"{schema}.{table}";

                // Return schema.table as key, actual tileKey in the url
                if (!simplifiedCatalog.ContainsKey(fullKey))
                {
                    simplifiedCatalog[fullKey] = new
                    {
                        schema,
                        table,
                        url = $"http://localhost:7800/{tileKey}/{{z}}/{{x}}/{{y}}"
                    };
                }
            }

            _logger.LogInformation("Catalog built for zoom {Zoom} with entries: {Keys}", zoom, string.Join(", ", simplifiedCatalog.Keys));
            return simplifiedCatalog;
        }
    }
    [Route("api/search")]
    public class SearchController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<SearchController> _logger;

        public SearchController(IConfiguration configuration, ILogger<SearchController> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        [HttpGet]
        public async Task<IActionResult> Search([FromQuery] string q)
        {
            if (string.IsNullOrWhiteSpace(q)) return BadRequest("Query cannot be empty");

            // Check if input looks like coordinates
            if (TryParseCoordinates(q, out double lon, out double lat))
            {
                return Ok(new { type = "coordinate", lon, lat });
            }

            // Fallback to placename search
            var result = await SearchPlaceName(q);
            return result != null
                ? Ok(new { type = "placename", name = result.Name, lon = result.Lon, lat = result.Lat })
                : NotFound("No match found");
        }

        private bool TryParseCoordinates(string input, out double lon, out double lat)
        {
            lon = lat = 0;
            var parts = input.Split(',', ' ', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 2) return false;

            return double.TryParse(parts[0], NumberStyles.Float, CultureInfo.InvariantCulture, out lon)
                && double.TryParse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture, out lat)
                && lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90;
        }

        private async Task<PlaceResult> SearchPlaceName(string query)
        {
            var connStr = _configuration.GetConnectionString("PostgresConnection");
            try
            {
                await using var conn = new NpgsqlConnection(connStr);
                await conn.OpenAsync();

                var cmd = new NpgsqlCommand(@"
            SELECT name, ST_X(way) AS lon, ST_Y(way) AS lat, place
            FROM planet_osm_point
            WHERE LOWER(name) ILIKE @q
            ORDER BY 
                CASE WHEN place = 'city' THEN 1 ELSE 2 END,
                name
            LIMIT 1
        ", conn);

                cmd.Parameters.AddWithValue("@q", "%" + query.ToLower() + "%");

                await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SingleRow);
                if (await reader.ReadAsync())
                {
                    return new PlaceResult
                    {
                        Name = reader.GetString(0),
                        Lon = reader.GetDouble(1),
                        Lat = reader.GetDouble(2)
                    };
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during placename search");
            }

            return null;
        }

        private class PlaceResult
        {
            public string Name { get; set; }
            public double Lon { get; set; }
            public double Lat { get; set; }
        }
        [HttpGet("suggest")]
        public async Task<IActionResult> Suggest([FromQuery] string q)
        {
            if (string.IsNullOrWhiteSpace(q)) return BadRequest("Query required");

            var connStr = _configuration.GetConnectionString("PostgresConnection");
            var results = new List<object>();

            try
            {
                await using var conn = new NpgsqlConnection(connStr);
                await conn.OpenAsync();

                var cmd = new NpgsqlCommand(@"
            SELECT DISTINCT name
            FROM planet_osm_point
            WHERE name ILIKE @q
            ORDER BY name
            LIMIT 10", conn);

                cmd.Parameters.AddWithValue("@q", $"{q}%");

                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    results.Add(reader.GetString(0));
                }

                return Ok(results);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during autocomplete suggest");
                return StatusCode(500);
            }
        }

    }
}
