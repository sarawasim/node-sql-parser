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
    return getDateFiltersFromBinaryExpression(whereClause, database)
  }
  // console.log("Filters: ", filters)
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

// function exploreExpressions(
//   expr: any,
//   filters: DateFilter[],
//   engine: DatabaseOpt
// ): void {
//   if (!expr) return

//   if (expr.type === "binary_expr") {
//     getDateFiltersFromBinaryExpression(expr, engine)
//   } else {
//     getDateFiltersFromFunction(expr, engine)
//   }
// }

function mergeFilters(filter1, filter2) {
  return {
    period: filter1?.period || filter2?.period,
    numberOfPeriods: filter1?.numberOfPeriods || filter2?.numberOfPeriods || 1,
    type: filter1?.type || filter2?.type,
    field: filter1?.field || filter2?.field,
  }
}

const operatorsUsedInMerge = ["<", "<=", ">", ">=", "=", "+", "-"]

function getDateFiltersFromBinaryExpression(
  expr: any,
  engine: DatabaseOpt
): DateFilter[] {
  const exprType = expr.type
  const column = getColumnName(expr, engine)
  const filters = []
  const filtersToProcess = []

  let period = undefined

  if (exprType === "extract") {
    exploreExtract(expr, engine)
  } else if (exprType === "binary_expr") {
    const left = getDateFiltersFromBinaryExpression(expr.left, engine)
    const right = getDateFiltersFromBinaryExpression(expr.right, engine)

    if (operatorsUsedInMerge.includes(expr.operator)) {
      const merge = mergeFilters(left[left.length - 1], right[right.length - 1])
      filtersToProcess.push(merge)
    } else {
      filtersToProcess.push(...left)
      filtersToProcess.push(...right)
    }
  } else if (exprType === "function") {
    filtersToProcess.push(...getDateFiltersFromFunction(expr, engine))
  } else if (exprType === "interval") {
    period = getPeriod(expr, engine)
  }
  filtersToProcess.forEach((filter) => {
    if (!filter.field) {
      filter.field = column
    }
    if (!filter.period) {
      filter.period = period?.period
    }
    if (!filter.numberOfPeriods) {
      filter.numberOfPeriods = period?.numberOfPeriods
    }
    filters.push(filter)
  })
  if (filters.length === 0 && (column || period)) {
    filters.push({
      field: column,
      period: period?.period,
      numberOfPeriods: period?.numberOfPeriods,
    })
  }

  return filters
}

function getDateFiltersFromFunction(
  expr: any,
  engine: DatabaseOpt
): DateFilter[] {
  const filters = []
  const filtersToProcess = []
  let column = ""
  let period = ""

  expr.args?.value.forEach((arg) => {
    if (arg.type === "binary_expr") {
      filtersToProcess.push(...getDateFiltersFromBinaryExpression(arg, engine))
    } else if (arg.type === "function") {
      filtersToProcess.push(...getDateFiltersFromFunction(arg, engine))
    } else if (arg.type === "column_ref") {
      column = getColumnName(arg, engine)
    } else if (arg.type === "single_quote_string") {
      period = getPeriod(arg, engine)
    } else if (arg.type === "interval") {
      period = getPeriod(arg, engine)
    }
  })

  filtersToProcess.forEach((filter) => {
    if (!filter.field) {
      filter.field = column
    }
    if (!filter.period) {
      filter.period = period?.period
    }
    if (!filter.numberOfPeriods) {
      filter.numberOfPeriods = period?.numberOfPeriods
    }
    filters.push(filter)
  })
  if (filters.length === 0 && (column || period)) {
    filters.push({
      field: column,
      period: period?.period,
      numberOfPeriods: period?.numberOfPeriods,
    })
  }
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

function getPeriod(expr: any, engine: DatabaseOpt) {
  if (expr.type === "interval") {
    return getPeriod(expr.expr)
  }
  if (expr.type !== "single_quote_string") return
  const split = expr.value.split(" ")
  let numberOfPeriods = undefined
  if (split.length > 1) {
    numberOfPeriods = parseInt(split[0])
  }
  const period = split[split.length - 1]
  return { numberOfPeriods, period }
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
  if (!expr.args) return
  Object.entries(expr.args.value).forEach((arg) => {
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

const sqlQuery = `SELECT *
FROM transactions
WHERE EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month')
  AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')`

const filters = getDateFiltersFromSQLQuery({ sqlQuery, database: "postgresql" })
console.log(filters)
