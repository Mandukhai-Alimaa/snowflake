// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const (
	unionTypeInt64   arrow.UnionTypeCode = 0
	unionTypeUint64  arrow.UnionTypeCode = 1
	unionTypeFloat64 arrow.UnionTypeCode = 2
	unionTypeBinary  arrow.UnionTypeCode = 3
)

// Custom Snowflake-specific statistic keys
const (
	SnowflakeStatisticBytesKey  = 1024
	SnowflakeStatisticBytesName = "snowflake.table.bytes"

	SnowflakeStatisticRetentionTimeDaysKey = 1025
	SnowflakeStatisticRetentionTimeName    = "snowflake.table.retention.time_days"

	SnowflakeStatisticActiveBytesKey  = 1026
	SnowflakeStatisticActiveBytesName = "snowflake.table.bytes.active"

	SnowflakeStatisticTimeTravelBytesKey  = 1027
	SnowflakeStatisticTimeTravelBytesName = "snowflake.table.bytes.time_travel"

	SnowflakeStatisticFailsafeBytesKey  = 1028
	SnowflakeStatisticFailsafeBytesName = "snowflake.table.bytes.failsafe"

	SnowflakeStatisticClusteringDepthKey  = 1029
	SnowflakeStatisticClusteringDepthName = "snowflake.table.clustering.depth"
)

const (
	// maxExactTables is the maximum number of tables for which we'll fetch expensive exact statistics.
	// When approximate=false and the candidate count exceeds this limit, we return StatusNotImplemented
	// to avoid performance cliffs on large Snowflake accounts.
	maxExactTables = 1000
)

type snowflakeStatistic struct {
	tableName  string
	columnName *string
	key        int16
	valueKind  arrow.UnionTypeCode
	valueI64   int64
	valueU64   uint64
	valueF64   float64
	valueBin   []byte
	approx     bool
}

func newInt64Stat(table string, column *string, key int16, value int64, approx bool) snowflakeStatistic {
	return snowflakeStatistic{
		tableName:  table,
		columnName: column,
		key:        key,
		valueKind:  unionTypeInt64,
		valueI64:   value,
		approx:     approx,
	}
}

func newFloat64Stat(table string, column *string, key int16, value float64, approx bool) snowflakeStatistic {
	return snowflakeStatistic{
		tableName:  table,
		columnName: column,
		key:        key,
		valueKind:  unionTypeFloat64,
		valueF64:   value,
		approx:     approx,
	}
}

func (c *connectionImpl) GetStatisticNames(ctx context.Context) (array.RecordReader, error) {
	_, span := driverbase.StartSpan(ctx, "connectionImpl.GetStatisticNames", c)
	defer driverbase.EndSpan(span, nil)

	statistics := []struct {
		Name string
		Key  int16
	}{
		{SnowflakeStatisticBytesName, SnowflakeStatisticBytesKey},
		{SnowflakeStatisticRetentionTimeName, SnowflakeStatisticRetentionTimeDaysKey},
		{SnowflakeStatisticActiveBytesName, SnowflakeStatisticActiveBytesKey},
		{SnowflakeStatisticTimeTravelBytesName, SnowflakeStatisticTimeTravelBytesKey},
		{SnowflakeStatisticFailsafeBytesName, SnowflakeStatisticFailsafeBytesKey},
		{SnowflakeStatisticClusteringDepthName, SnowflakeStatisticClusteringDepthKey},
	}

	bldr := array.NewRecordBuilder(c.Alloc, adbc.GetStatisticNamesSchema)
	defer bldr.Release()

	nameBldr := bldr.Field(0).(*array.StringBuilder)
	keyBldr := bldr.Field(1).(*array.Int16Builder)

	for _, stat := range statistics {
		nameBldr.Append(stat.Name)
		keyBldr.Append(stat.Key)
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return array.NewRecordReader(adbc.GetStatisticNamesSchema, []arrow.RecordBatch{rec})
}

// GetStatistics returns table statistics from INFORMATION_SCHEMA.TABLES.
// Performance considerations:
//   - Exact mode (approximate=false): Fails with StatusNotImplemented if candidate count exceeds 1,000 tables.
//     Intended for narrow, targeted queries where detailed statistics justify the cost.
//   - Approximate mode (approximate=true): No limit on candidate count. Designed to handle arbitrarily
//     large result sets (e.g., account-wide scans) by returning only cheap base statistics.
//
// The approximate parameter controls query cost:
//   - Base statistics (row_count, bytes, retention_time) are always sourced from cached
//     INFORMATION_SCHEMA.TABLES (~30 min staleness) regardless of the approximate parameter,
//     and are always marked as approximate in the result.
//   - When approximate=true: Skip expensive per-table queries (TABLE_STORAGE_METRICS, SYSTEM$CLUSTERING_INFORMATION)
//   - When approximate=false: Include storage breakdown and clustering depth for matched tables;
//     these enriched statistics are marked as exact (approx=false).
func (c *connectionImpl) GetStatistics(
	ctx context.Context,
	catalog *string,
	dbSchema *string,
	tableName *string,
	approximate bool,
) (rdr array.RecordReader, err error) {
	ctx, span := driverbase.StartSpan(ctx, "connectionImpl.GetStatistics", c)
	defer func() {
		driverbase.EndSpan(span, err)
	}()

	// Build filter parameters (default to '%' for NULL to match all)
	schemaFilter := "%"
	if dbSchema != nil {
		schemaFilter = *dbSchema
	}
	tableFilter := "%"
	if tableName != nil {
		tableFilter = *tableName
	}

	if (catalog != nil && *catalog == "") || (dbSchema != nil && *dbSchema == "") || (tableName != nil && *tableName == "") {
		return c.emptyGetStatisticsReader()
	}

	// Determine which databases to query:
	var databasesToQuery []string

	if catalog != nil && !strings.ContainsAny(*catalog, "%_") {
		// Catalog is literal, query only that specific database
		databasesToQuery = []string{*catalog}
	} else {
		var query string
		if catalog == nil {
			query = "SHOW DATABASES"
		} else {
			query = fmt.Sprintf("SHOW DATABASES LIKE '%s'", strings.ReplaceAll(*catalog, "'", "''"))
		}

		rows, err := c.cn.QueryContext(ctx, query, nil)
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}
		defer func() {
			err = errors.Join(err, rows.Close())
		}()

		columns := rows.Columns()
		numColumns := len(columns)
		if numColumns < 2 {
			return nil, errToAdbcErr(adbc.StatusInternal, fmt.Errorf("SHOW DATABASES returned %d columns, expected at least 2", numColumns))
		}

		dest := make([]driver.Value, numColumns)
		for {
			if err := rows.Next(dest); err != nil {
				if err == io.EOF {
					break
				}
				return nil, errToAdbcErr(adbc.StatusIO, err)
			}
			if dest[1] != nil {
				if dbName, ok := dest[1].(string); ok {
					databasesToQuery = append(databasesToQuery, dbName)
				}
			}
		}
	}

	if len(databasesToQuery) == 0 {
		return c.emptyGetStatisticsReader()
	}

	// Query tables from each matching database and group by catalog
	// In exact mode, track total count and fail fast if limit exceeded
	tablesByCatalog := make(map[string][]tableInfo)
	catalogOrder := []string{}
	totalTables := 0

	for _, db := range databasesToQuery {
		quotedDB := quoteIdentifier(db)

		query := fmt.Sprintf(`SELECT table_catalog, table_schema, table_name,
                     COALESCE(row_count, 0) as row_count,
                     COALESCE(bytes, 0) as bytes,
                     COALESCE(retention_time, 1) as retention_time,
                     clustering_key
              FROM %s.information_schema.tables
              WHERE table_schema ILIKE ?
                AND table_name ILIKE ?
                AND table_type = 'BASE TABLE'
              ORDER BY table_schema, table_name`, quotedDB)

		nvargs := []driver.NamedValue{
			{Ordinal: 1, Value: schemaFilter},
			{Ordinal: 2, Value: tableFilter},
		}

		rows, err := c.cn.QueryContext(ctx, query, nvargs)
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, fmt.Errorf("failed to query database %s: %w", db, err))
		}

		tables, err := readTableRows(rows)
		err = errors.Join(err, rows.Close())

		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, fmt.Errorf("failed to read table rows from database %s: %w", db, err))
		}

		if len(tables) > 0 {
			actualCatalog := tables[0].catalog
			if _, exists := tablesByCatalog[actualCatalog]; !exists {
				catalogOrder = append(catalogOrder, actualCatalog)
			}
			tablesByCatalog[actualCatalog] = append(tablesByCatalog[actualCatalog], tables...)
			totalTables += len(tables)

			// Early bailout in exact mode: fail fast once we exceed the limit
			// Approximate mode has no limit - it's designed to handle arbitrarily large result sets
			if !approximate && totalTables > maxExactTables {
				return nil, adbc.Error{
					Code: adbc.StatusNotImplemented,
					Msg: fmt.Sprintf(
						"exact statistics requested for more than %d tables; use approximate=true or narrower catalog/schema/table filters",
						maxExactTables,
					),
				}
			}
		}
	}

	if len(tablesByCatalog) == 0 {
		return c.emptyGetStatisticsReader()
	}

	// Build statistics for each catalog
	schemaOrderByCatalog := make(map[string][]string)
	statsByCatalog := make(map[string]map[string][]snowflakeStatistic)

	for _, catalogName := range catalogOrder {
		tables := tablesByCatalog[catalogName]

		// Query storage metrics for this specific catalog (expensive, skip if approximate=true)
		var storageMetrics map[string]map[string]storageMetric
		if !approximate {
			var err error
			storageMetrics, err = queryStorageMetrics(ctx, c.cn, catalogName, tables)
			if err != nil {
				// Storage metrics are optional (requires ACCOUNTADMIN)
				storageMetrics = make(map[string]map[string]storageMetric)
			}
		} else {
			storageMetrics = make(map[string]map[string]storageMetric)
		}

		// Build statistics grouped by schema
		statsBySchema := make(map[string][]snowflakeStatistic)
		schemaOrder := []string{}
		seenSchemas := make(map[string]bool)

		for _, ti := range tables {
			// Track schema order
			if !seenSchemas[ti.schema] {
				schemaOrder = append(schemaOrder, ti.schema)
				seenSchemas[ti.schema] = true
			}

			// Build base statistics (always included, always approximate since from cached INFORMATION_SCHEMA)
			stats := []snowflakeStatistic{
				newFloat64Stat(ti.table, nil, adbc.StatisticRowCountKey, float64(ti.rowCount), true),
				newInt64Stat(ti.table, nil, SnowflakeStatisticBytesKey, ti.bytes, true),
				newInt64Stat(ti.table, nil, SnowflakeStatisticRetentionTimeDaysKey, ti.retentionTime, true),
			}

			// Add storage breakdown if available and not in approximate mode
			if !approximate {
				if schemaMetrics, ok := storageMetrics[ti.schema]; ok {
					if sm, ok := schemaMetrics[ti.table]; ok {
						stats = append(stats,
							newInt64Stat(ti.table, nil, SnowflakeStatisticActiveBytesKey, sm.activeBytes, false),
							newInt64Stat(ti.table, nil, SnowflakeStatisticTimeTravelBytesKey, sm.timeTravelBytes, false),
							newInt64Stat(ti.table, nil, SnowflakeStatisticFailsafeBytesKey, sm.failsafeBytes, false),
						)
					}
				}
			}

			// Add clustering depth if clustering key exists (expensive, skip if approximate=true)
			if !approximate && ti.clusteringKey.Valid && ti.clusteringKey.String != "" {
				clusteringDepth, err := getClusteringDepth(ctx, c.cn, ti.catalog, ti.schema, ti.table)
				if err == nil {
					stats = append(stats, newFloat64Stat(ti.table, nil, SnowflakeStatisticClusteringDepthKey, clusteringDepth, false))
				}
			}

			statsBySchema[ti.schema] = append(statsBySchema[ti.schema], stats...)
		}

		schemaOrderByCatalog[catalogName] = schemaOrder
		statsByCatalog[catalogName] = statsBySchema
	}

	// Build Arrow structure from pre-computed statistics
	return c.buildMultiCatalogStatisticsReader(catalogOrder, schemaOrderByCatalog, statsByCatalog)
}

// tableInfo holds basic table information from INFORMATION_SCHEMA.TABLES
type tableInfo struct {
	catalog       string
	schema        string
	table         string
	rowCount      int64
	bytes         int64
	retentionTime int64
	clusteringKey sql.NullString
}

// readTableRows reads table information from query results
func readTableRows(rows driver.Rows) ([]tableInfo, error) {
	var tables []tableInfo
	dest := make([]driver.Value, 7)

	for {
		err := rows.Next(dest)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		catalog := dest[0].(string)
		schema := dest[1].(string)
		table := dest[2].(string)

		var rowCount, bytes, retentionTime int64
		if v := dest[3]; v != nil {
			rowCount = convertToInt64(v)
		}
		if v := dest[4]; v != nil {
			bytes = convertToInt64(v)
		}
		if v := dest[5]; v != nil {
			retentionTime = convertToInt64(v)
		}

		var clusteringKey sql.NullString
		if dest[6] != nil {
			if s, ok := dest[6].(string); ok {
				clusteringKey = sql.NullString{String: s, Valid: true}
			}
		}

		tables = append(tables, tableInfo{
			catalog:       catalog,
			schema:        schema,
			table:         table,
			rowCount:      rowCount,
			bytes:         bytes,
			retentionTime: retentionTime,
			clusteringKey: clusteringKey,
		})
	}

	return tables, nil
}

// storageMetric holds storage breakdown from TABLE_STORAGE_METRICS
type storageMetric struct {
	activeBytes     int64
	timeTravelBytes int64
	failsafeBytes   int64
}

// queryStorageMetrics queries INFORMATION_SCHEMA.TABLE_STORAGE_METRICS for storage breakdown
// Note: Requires appropriate permissions (ACCOUNTADMIN) and may return empty for other roles
func queryStorageMetrics(ctx context.Context, conn driver.QueryerContext, catalog string, tables []tableInfo) (map[string]map[string]storageMetric, error) {
	if len(tables) == 0 || catalog == "" {
		return make(map[string]map[string]storageMetric), nil
	}

	// Build a list of (schema, table) pairs to filter by
	var pairs []string
	for _, t := range tables {
		pairs = append(pairs, fmt.Sprintf("(%s, %s)",
			quoteLiteral(t.schema),
			quoteLiteral(t.table)))
	}

	// Build the query filtered to only candidate tables
	query := fmt.Sprintf(`
		SELECT table_schema, table_name,
		       COALESCE(active_bytes, 0) as active_bytes,
		       COALESCE(time_travel_bytes, 0) as time_travel_bytes,
		       COALESCE(failsafe_bytes, 0) as failsafe_bytes
		FROM %s.information_schema.table_storage_metrics
		WHERE (table_schema, table_name) IN (%s)
		ORDER BY table_schema, table_name`, quoteIdentifier(catalog), strings.Join(pairs, ", "))

	nvargs := []driver.NamedValue{}
	rows, err := conn.QueryContext(ctx, query, nvargs)
	if err != nil {
		// TABLE_STORAGE_METRICS may not be accessible (requires ACCOUNTADMIN)
		return nil, err
	}
	defer func() {
		if closer, ok := rows.(io.Closer); ok {
			_ = closer.Close()
		}
	}()

	result := make(map[string]map[string]storageMetric)
	dest := make([]driver.Value, 5)

	for {
		err := rows.Next(dest)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		schema := dest[0].(string)
		table := dest[1].(string)
		activeBytes := convertToInt64(dest[2])
		timeTravelBytes := convertToInt64(dest[3])
		failsafeBytes := convertToInt64(dest[4])

		if result[schema] == nil {
			result[schema] = make(map[string]storageMetric)
		}

		result[schema][table] = storageMetric{
			activeBytes:     activeBytes,
			timeTravelBytes: timeTravelBytes,
			failsafeBytes:   failsafeBytes,
		}
	}

	return result, nil
}

// getClusteringDepth queries SYSTEM$CLUSTERING_INFORMATION to get the average clustering depth
func getClusteringDepth(ctx context.Context, conn driver.QueryerContext, catalog, schema, table string) (float64, error) {
	// Call SYSTEM$CLUSTERING_INFORMATION function
	// When the table has a clustering key, call without second argument
	// The function will use the table's existing clustering key
	fullyQualifiedTable := fmt.Sprintf("%s.%s.%s",
		quoteIdentifier(catalog),
		quoteIdentifier(schema),
		quoteIdentifier(table))

	query := fmt.Sprintf("SELECT SYSTEM$CLUSTERING_INFORMATION(%s)", quoteLiteral(fullyQualifiedTable))

	nvargs := []driver.NamedValue{}
	rows, err := conn.QueryContext(ctx, query, nvargs)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closer, ok := rows.(io.Closer); ok {
			_ = closer.Close()
		}
	}()

	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		return 0, err
	}

	// Parse JSON result - SYSTEM$CLUSTERING_INFORMATION returns JSON string
	if dest[0] == nil {
		return 0, fmt.Errorf("null result from SYSTEM$CLUSTERING_INFORMATION")
	}

	jsonStr, ok := dest[0].(string)
	if !ok {
		return 0, fmt.Errorf("unexpected type from SYSTEM$CLUSTERING_INFORMATION: %T", dest[0])
	}

	var result struct {
		AverageDepth float64 `json:"average_depth"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		return 0, err
	}

	return result.AverageDepth, nil
}

// buildMultiCatalogStatisticsReader constructs the Arrow RecordReader for GetStatistics with multiple catalogs.
// This is a pure Arrow building function with no business logic or database queries, making it suitable
// for extraction to a shared library in the future.
func (c *connectionImpl) buildMultiCatalogStatisticsReader(
	catalogOrder []string,
	schemaOrder map[string][]string,
	statsByCatalog map[string]map[string][]snowflakeStatistic,
) (array.RecordReader, error) {
	bldr := array.NewRecordBuilder(c.Alloc, adbc.GetStatisticsSchema)
	defer bldr.Release()

	// Get all field builders
	catalogNameBldr := bldr.Field(0).(*array.StringBuilder)
	catalogSchemasBldr := bldr.Field(1).(*array.ListBuilder)
	dbSchemaStructBldr := catalogSchemasBldr.ValueBuilder().(*array.StructBuilder)
	dbSchemaNameBldr := dbSchemaStructBldr.FieldBuilder(0).(*array.StringBuilder)
	dbSchemaStatsListBldr := dbSchemaStructBldr.FieldBuilder(1).(*array.ListBuilder)

	statsStructBldr := dbSchemaStatsListBldr.ValueBuilder().(*array.StructBuilder)
	tableNameBldr := statsStructBldr.FieldBuilder(0).(*array.StringBuilder)
	columnNameBldr := statsStructBldr.FieldBuilder(1).(*array.StringBuilder)
	statKeyBldr := statsStructBldr.FieldBuilder(2).(*array.Int16Builder)
	statValueBldr := statsStructBldr.FieldBuilder(3).(*array.DenseUnionBuilder)
	statApproxBldr := statsStructBldr.FieldBuilder(4).(*array.BooleanBuilder)

	statI64Bldr := statValueBldr.Child(0).(*array.Int64Builder)
	statU64Bldr := statValueBldr.Child(1).(*array.Uint64Builder)
	statF64Bldr := statValueBldr.Child(2).(*array.Float64Builder)
	statBinBldr := statValueBldr.Child(3).(*array.BinaryBuilder)

	// Build Arrow structure from pre-computed statistics
	for _, catalogName := range catalogOrder {
		catalogNameBldr.Append(catalogName)
		catalogSchemasBldr.Append(true)

		for _, schema := range schemaOrder[catalogName] {
			dbSchemaStructBldr.Append(true)
			dbSchemaNameBldr.Append(schema)
			dbSchemaStatsListBldr.Append(true)

			for _, st := range statsByCatalog[catalogName][schema] {
				statsStructBldr.Append(true)
				tableNameBldr.Append(st.tableName)

				if st.columnName == nil {
					columnNameBldr.AppendNull()
				} else {
					columnNameBldr.Append(*st.columnName)
				}

				statKeyBldr.Append(st.key)
				statApproxBldr.Append(st.approx)

				statValueBldr.Append(st.valueKind)
				switch st.valueKind {
				case unionTypeInt64:
					statI64Bldr.Append(st.valueI64)
				case unionTypeUint64:
					statU64Bldr.Append(st.valueU64)
				case unionTypeFloat64:
					statF64Bldr.Append(st.valueF64)
				case unionTypeBinary:
					statBinBldr.Append(st.valueBin)
				default:
					return nil, c.ErrorHelper.Errorf(adbc.StatusInternal, "unknown statistic value kind: %d", st.valueKind)
				}
			}
		}
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return array.NewRecordReader(adbc.GetStatisticsSchema, []arrow.RecordBatch{rec})
}

// emptyGetStatisticsReader returns an empty GetStatistics result
func (c *connectionImpl) emptyGetStatisticsReader() (array.RecordReader, error) {
	bldr := array.NewRecordBuilder(c.Alloc, adbc.GetStatisticsSchema)
	defer bldr.Release()

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return array.NewRecordReader(adbc.GetStatisticsSchema, []arrow.RecordBatch{rec})
}

// convertToInt64 converts various numeric types to int64
func convertToInt64(v any) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case float64:
		return int64(val)
	case string:
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			return parsed
		}
	}
	return 0
}

// quoteIdentifier quotes a Snowflake identifier to handle special characters and preserve case
func quoteIdentifier(identifier string) string {
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}

// quoteLiteral quotes a SQL string literal for use in WHERE clauses
func quoteLiteral(value string) string {
	escaped := strings.ReplaceAll(value, `'`, `''`)
	return fmt.Sprintf(`'%s'`, escaped)
}
