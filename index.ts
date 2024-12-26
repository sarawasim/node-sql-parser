import { getDateFiltersFromSQLQuery } from "./src/dateFilterExtractor.js"

const sqlQuery = `SELECT *
FROM transactions
WHERE
    DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE)`

const filters = getDateFiltersFromSQLQuery({ sqlQuery, database: "bigquery" })
console.log(filters)
