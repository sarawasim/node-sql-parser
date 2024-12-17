import { DateFilter } from "./dateFilter.js"
import { getPostgresqlFilters } from "./postgres/postgresqlParser.js"
import { getMysqlFilters } from "./mysql/mysqlParser.js"
import { getSnowflakeFilters } from "./snowflake/snowflakeParser.js"
import { getRedshiftFilters } from "./redshift/redshiftParser.js"
import { getBigQueryFilters } from "./bigquery/bigqueryParser.js"

type DatabaseOpt =
  | "postgresql"
  | "mysql"
  | "snowflake"
  | "redshift"
  | "bigquery"

export function getDateFiltersFromSQLQuery({
  sqlQuery,
  database,
}: {
  sqlQuery: string
  database: DatabaseOpt
}): DateFilter[] {
  let sqlParser
  switch (database) {
    case "postgresql":
      sqlParser = getPostgresqlFilters
      break
    case "mysql":
      sqlParser = getMysqlFilters
      break
    case "snowflake":
      sqlParser = getSnowflakeFilters
      break
    case "redshift":
      sqlParser = getRedshiftFilters
      break
    case "bigquery":
      sqlParser = getBigQueryFilters
      break
    default:
      sqlParser = getPostgresqlFilters
      break
  }
  return sqlParser(sqlQuery)
}
