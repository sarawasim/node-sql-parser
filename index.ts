import { getDateFiltersFromSQLQuery } from "./src/sqlParser.js"

//current
// const sqlQuery =
//   "SELECT * FROM transactions WHERE DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE)"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "postgresql" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE)"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "postgresql" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE created_at >= date_trunc('month', CURRENT_DATE) AND created_at < (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month')"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "postgresql" })

// last *******
// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE - INTERVAL '3 months'"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "postgresql" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 months'"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "postgresql" })

//type previous in datefilter
// const sqlQuery =
//   "SELECT * FROM transactions WHERE DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "postgresql" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE DATE_TRUNC('quarter', transaction_date) = DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '1 quarter')"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "postgresql" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month') AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE + INTERVAL '1 month')"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "postgresql" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE id = 10 AND (transaction_date > '2024-12-12' OR (transaction_date < CURRENT_DATE - INTERVAL '1 month' AND id > 20))"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "postgresql" })

//++++++++++ mysl ++++++++++
// const sqlQuery =
//   "SELECT * FROM transactions WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE()) AND MONTH(transaction_date) = MONTH(CURRENT_DATE())"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "mysql" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE YEAR(transaction_date) = YEAR(CURDATE())"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "mysql" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= CURDATE() - INTERVAL 1 MONTH"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "mysql" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "mysql" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(NOW(), INTERVAL 1 MONTH)"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "mysql" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "mysql" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= CURDATE() - INTERVAL 90 DAY"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "mysql" })

//++++++++++ snowflake ++++++++++
// const sqlQuery =
//   "SELECT * FROM transactions WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE()) AND MONTH(transaction_date) = MONTH(CURRENT_DATE())"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "snowflake" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE())"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "snowflake" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE()) AND MONTH(transaction_date) = MONTH(CURRENT_DATE())"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "snowflake" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) AND EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE())"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "snowflake" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE() - INTERVAL '1 YEAR'"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "snowflake" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATEADD(YEAR, -1, CURRENT_DATE())"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "snowflake" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE() - INTERVAL '90 DAY'"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "snowflake" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE() - INTERVAL '6 MONTH'"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "snowflake" })

// **************
// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATEADD(DAY, -90, CURRENT_DATE())"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "snowflake" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATE_TRUNC('MONTH', CURRENT_DATE() - INTERVAL '1 MONTH') AND transaction_date < DATE_TRUNC('MONTH', CURRENT_DATE())"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "snowflake" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATEADD(month, -1, DATE_TRUNC('MONTH', CURRENT_DATE())) AND transaction_date < DATE_TRUNC('MONTH', CURRENT_DATE())"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "snowflake" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE() - INTERVAL '1 MONTH') AND LAST_DAY(DATE_TRUNC('MONTH', CURRENT_DATE() - INTERVAL '1 MONTH'))"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "snowflake" })

//++++++++++ bigquery ++++++++++
// const sqlQuery =
//   "SELECT * FROM transactions WHERE EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) AND EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE())"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "bigquery" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "bigquery" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "bigquery" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "bigquery" })

//**********
// const sqlQuery =
//   'SELECT * FROM transactions WHERE PARSE_TIMESTAMP("%Y-%M-%D", transaction_date) = PARSE_TIMESTAMP("%Y-%M-%D", CURRENT_DATE())'
// getDateFiltersFromSQLQuery({ sqlQuery, database: "bigquery" })

//**********
// const sqlQuery =
//   "SELECT * FROM transactions WHERE TIMESTAMP_TRUNC(dateField, INTERVAL MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "bigquery" })

//++++++++++ redshift ++++++++++
// const sqlQuery =
//   "SELECT * FROM transactions WHERE EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE) AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE)"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "redshift" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATE_TRUNC('month', CURRENT_DATE) AND transaction_date < (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month')"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "redshift" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATEADD(day, -90, GETDATE())"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "redshift" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATEADD(day, -90, CURRENT_DATE)"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "redshift" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATEADD(day, -90, GETDATE())"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "redshift" })

// const sqlQuery =
//   "SELECT * FROM transactions WHERE transaction_date >= DATEADD(day, -90, CURRENT_DATE)"
// getDateFiltersFromSQLQuery({ sqlQuery, database: "redshift" })

const sqlQuery =
  "SELECT * FROM transactions WHERE transaction_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND transaction_date < DATE_TRUNC('month', CURRENT_DATE)"
getDateFiltersFromSQLQuery({ sqlQuery, database: "redshift" })
