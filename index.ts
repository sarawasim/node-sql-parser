import { getDateFiltersFromSQLQuery } from "./src/sqlParser.js"

const sqlQuery =
  "SELECT * FROM transactions WHERE transaction_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND transaction_date < DATE_TRUNC('month', CURRENT_DATE)"
getDateFiltersFromSQLQuery({ sqlQuery, database: "redshift" })
