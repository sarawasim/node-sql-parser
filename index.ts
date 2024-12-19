import { DateFilter } from "./src/dateFilter.js"
import pkg from "node-sql-parser"
const { Parser } = pkg

type DatabaseOpt =
  | "postgresql"
  | "mysql"
  | "snowflake"
  | "redshift"
  | "bigquery"

const paths = {
  mysql: {
    column: "type.column_ref",
  },
  postgresql: {
    column_ref: {
      column: "column.expr.value",
      functions: {
        date_trunc: "args.value[1].column.expr.value",
      },
      extract: "args.source.column.expr.value",
    },
    function: {
      name: "name.name[0].value",
    },
  },
}

function getDateFiltersFromSQLQuery({
  sqlQuery,
  database,
}: {
  sqlQuery: string
  database: DatabaseOpt
}): DateFilter[] {
  const parser = new Parser()
  const ast = parser.astify(sqlQuery, { database })
  if (!ast || Array.isArray(ast)) return []
  const filters: DateFilter[] = []
  // @ts-ignore
  const whereClause = ast.where

  console.log(JSON.stringify(whereClause, null, 2))

  if (whereClause) {
    exploreExpressions(whereClause, filters, database)
  }
  console.log("Filters: ", filters)
  return filters
}

function getNestedValue(obj: any, path: string): any {
  const nestedValue = path.split(".").reduce((acc, key) => {
    if (key.includes("[") && key.includes("]")) {
      const [baseKey, index] = key.split("[")
      const arrayIndex = parseInt(index.split("]")[0])
      acc = acc ? acc[baseKey] : undefined
      return acc ? acc[arrayIndex] : undefined
    } else {
      return acc ? acc[key] : undefined
    }
  }, obj)

  return nestedValue
}

function exploreExpressions(
  expr: any,
  filters: DateFilter[],
  engine: DatabaseOpt
): void {
  if (!expr) return

  if (expr.type === "binary_expr") {
    getDateFiltersFromBinaryExpression(expr, engine)
  } else {
    getDateFiltersFromFunction(expr, engine)
  }
}

function getDateFiltersFromBinaryExpression(
  expr: any,
  engine: DatabaseOpt
): DateFilter[] {
  const exprType = expr.type
  const leftType = expr.left?.type
  const rightType = expr.right?.type
  const columnRef = getColumnName(expr, engine)
  const filters = []
  console.log("columnRef: ", columnRef)

  if (exprType === "extract") {
    exploreExtract(expr, engine)
  } else if (exprType === "binary_expr") {
    if (leftType === "binary_expr") {
      filters.push(...getDateFiltersFromBinaryExpression(expr.left, engine))
    }
    if (rightType === "binary_expr") {
      filters.push(...getDateFiltersFromBinaryExpression(expr.right, engine))
    }
  } else if (exprType === "function") {
    filters.push(...getDateFiltersFromFunction(expr, engine))
  }
  return filters
}

function getDateFiltersFromFunction(
  expr: any,
  engine: DatabaseOpt
): DateFilter[] {
  const filters = []
  let column = ""
  let period = ""
  Object.entries(expr.args?.value).forEach((arg) => {
    if (arg.type === "binary_expr") {
      filters.push(...getDateFiltersFromBinaryExpression(arg, engine))
    } else if (arg.type === "function") {
      filters.push(...getDateFiltersFromFunction(arg, engine))
    } else if (arg.type === "column_ref") {
      column = getColumnName(arg, engine)
    } else if (arg.type === "single_quote_string") {
      period = getPeriod(arg, engine)
    }
  })
  return filters
}

function exploreExtract(expr: any, filters: DateFilter[], engine: DatabaseOpt) {
  if (!expr) return
  let columnRef = getColumnName(expr, engine)
  if (expr.args.source.type === "binary_expr") {
    getDateFiltersFromBinaryExpression(expr.args.source, filters, engine)
  }
  return columnRef
}

function parseDateFilter(expr: any, engine: DatabaseOpt): DateFilter | null {
  const type = getType(expr.operator)
  const numberOfPeriods = getNumberOfPeriods(expr, engine)
  const period = getPeriod(expr, engine)

  return { type, numberOfPeriods, period, field: "" }
}

function getType(
  operator: string
): "current" | "last" | "next" | "previous" | null {
  switch (operator) {
    case "=":
      return "current"
    case "-":
      return "last"
    case "+":
      return "next"
    default:
      return null
  }
}

function getNumberOfPeriods(expr: any, engine: DatabaseOpt): number {
  if (expr.type === "binary_expr" && expr.right.type === "interval") {
    const intervalNumber = expr.right.expr.value
    return parseInt(intervalNumber)
  }
  return 1
}

function getPeriod(expr: any, engine: DatabaseOpt) {
  if (expr.type === "binary_expr") {
    if (expr.right && expr.right.expr) {
      const intervalString = expr.right.expr.value
      return getPeriodFormatted(intervalString.split(" ")[1])
    }
  }
  if (
    expr.type === "function" &&
    expr.name.name[0].value.toLowerCase() === "date_trunc"
  ) {
    const intervalString = expr.args.value[0].value
    return getPeriodFormatted(intervalString)
  }
}

/*
const paths = {
  mysql: {
    column: "type.column_ref",
  },
  postgresql: {
    column_ref: {
      column: "column.expr.value",
      functions: {
        date_trunc: "args.value[1].column.expr.value",
      },
      extract: "args.source.column.expr.value",
      binary_expr: "column.value",
    },
    function: {
      name: "name.name[0].value",
    },
  },
}

*/

function getColumnName(expr: any, engine: DatabaseOpt): string | undefined {
  const exprType = expr.type

  switch (exprType) {
    case "binary_expr":
      return getColumnNameFromBinaryExpression(expr, engine)
    case "function":
      return getColumnNameFromFunction(expr, engine)
    case "column_ref":
      const path = paths[engine]?.column_ref.column
      return getNestedValue(expr, path)
    case "extract":
      return getColumnNameFromExtract(expr, engine)
    default:
      return undefined
  }
}

function getColumnNameFromBinaryExpression(
  expr: any,
  engine: DatabaseOpt
): string | undefined {
  if (expr.left.type === "column_ref") {
    console.log(paths[engine].column_ref.binary_expr)
    return getNestedValue(expr.left, paths[engine].column_ref.column)
  } else if (expr.right.type === "column_ref") {
    return getNestedValue(expr.right, paths[engine].column_ref.column)
  }
  return undefined
}

function getColumnNameFromFunction(
  expr: any,
  engine: DatabaseOpt
): string | undefined {
  Object.entries(expr.args?.value).forEach((arg) => {
    if (arg.type === "column_ref") {
      const functionName = getNestedValue(
        expr,
        paths[engine].function.name
      )?.toLowerCase()
      return getNestedValue(
        expr,
        paths[engine].column_ref.functions[functionName]
      )
    }
  })
  return undefined
}

function getColumnNameFromExtract(expr: any, engine: DatabaseOpt) {
  if (expr.args.source.type === "column_ref") {
    return getNestedValue(expr, paths[engine].extract)
  }
  return undefined
}

function getPeriodFormatted(period: string) {
  switch (period) {
    case "day":
    case "days":
      return "days"
    case "week":
    case "weeks":
      return "weeks"
    case "month":
    case "months":
      return "months"
    case "quarter":
    case "quarters":
      return "quarters"
  }
}

const sqlQuery =
  //
  // `SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE - INTERVAL '1 month'`

  //   `SELECT *
  // FROM transactions
  // WHERE DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')`

  `SELECT *
FROM transactions
WHERE DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')`

//   `SELECT *
// FROM transactions
// WHERE EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')
//   AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')`

getDateFiltersFromSQLQuery({ sqlQuery, database: "postgresql" })
