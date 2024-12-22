import { DateFilter } from "./src/dateFilter.js"
import pkg from "node-sql-parser"
const { Parser } = pkg

type DatabaseOpt =
  | "postgresql"
  | "mysql"
  | "snowflake"
  | "redshift"
  | "bigquery"

type Period = "days" | "weeks" | "months" | "quarters" | "years" | undefined

type PeriodObj = {
  period: Period | undefined
  numberOfPeriods: number | undefined
}

const operatorsUsedInMerge = [
  "<",
  "<=",
  ">",
  ">=",
  "=",
  "+",
  "-",
  "AND",
  "BETWEEN",
]

const timeMapper = {
  second: 1,
  seconds: 1,
  minute: 60,
  minutes: 60,
  hour: 3600,
  hours: 3600,
  day: 86400,
  days: 86400,
  week: 604800,
  weeks: 604800,
  month: 2592000,
  months: 2592000,
  quarter: 1036800,
  quarters: 1036800,
  year: 31536000,
  years: 31536000,
}

const paths = {
  redshift: {
    column_ref: {
      column: "column.expr.value",
      functions: {},
      extract: "args.source.column.expr.value",
    },
    function: {
      name: "name.name[0].value",
    },
    functions: {
      dateadd: {
        period: "args.value[0].column.expr.value",
        interval: "args.value[1].value",
      },
    },
  },
  snowflake: {
    column_ref: {
      column: "column",
      functions: {
        year: "args.value[1].column",
        month: "args.value[1].column",
      },
      extract: "args.source.column",
    },
    function: {
      name: "name.name[0].value",
    },
    functions: {
      dateadd: {
        period: "args.value[0].column",
        interval: "args.value[1].value",
      },
    },
  },
  bigquery: {
    column_ref: {
      column: "column",
      functions: {
        parse_timestamp: "args.value[1].column",
        timestamp_trunc: "args.value[0].column",
      },
      extract: "args.source.column",
    },
    function: {
      name: "name.schema.value",
    },
    functions: {},
  },
  mysql: {
    column_ref: {
      column: "column",
      functions: {
        year: "args.value[1].column",
        month: "args.value[1].column",
      },
    },
    function: {
      name: "name.name[0].value",
    },
    functions: {},
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
    functions: {},
  },
}

export function getDateFiltersFromSQLQuery({
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

  // console.log(JSON.stringify(whereClause, null, 2))
  if (whereClause) {
    const filtersToProcess = getDateFiltersFromBinaryExpression(
      whereClause,
      database
    )
    filtersToProcess.forEach((filter) => {
      if (filter.period && filter.field) {
        if (!filter.numberOfPeriods) {
          filter.numberOfPeriods = 1
        }
        filters.push(filter)
      }
    })
  }

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

function mergeFilters(filter1, filter2) {
  const refinedPeriod = getRefinedPeriod(filter1, filter2)
  return {
    type: filter1?.type || filter2?.type,
    numberOfPeriods: refinedPeriod.numberOfPeriods,
    period: refinedPeriod.period,
    field: filter1?.field || filter2?.field,
  }
}

function getRefinedPeriod(filter1, filter2) {
  if (!filter1 && !filter2) return {}
  if (!filter1) return filter2
  if (!filter2) return filter1

  if (!filter1.period) {
    return {
      period: filter2.period,
      numberOfPeriods: filter2.numberOfPeriods || filter1.numberOfPeriods,
    }
  }
  if (!filter2.period) {
    return {
      period: filter1.period,
      numberOfPeriods: filter1.numberOfPeriods || filter2.numberOfPeriods,
    }
  }
  const duration1 = (filter1.numberOfPeriods || 1) * timeMapper[filter1.period]
  const duration2 = (filter2.numberOfPeriods || 1) * timeMapper[filter2.period]

  if (duration1 < duration2) {
    return {
      period: filter1.period,
      numberOfPeriods: filter1.numberOfPeriods,
    }
  } else if (duration2 < duration1) {
    return {
      period: filter2.period,
      numberOfPeriods: filter2.numberOfPeriods,
    }
  } else {
    if (filter2.numberOfPeriods < filter1.numberOfPeriods) {
      return {
        period: filter2.period,
        numberOfPeriods: filter2.numberOfPeriods,
      }
    }
    return {
      period: filter1.period,
      numberOfPeriods: filter1.numberOfPeriods,
    }
  }
}

function getDateFiltersFromBinaryExpression(
  expr: any,
  engine: DatabaseOpt
): DateFilter[] {
  const column: string | undefined = getColumnName(expr, engine)
  const filters: DateFilter[] = []
  const filtersToProcess: DateFilter[] = []

  let period: PeriodObj | undefined

  switch (expr.type) {
    case "extract":
      filtersToProcess.push(...getDateFiltersFromExtract(expr, engine))
      break

    case "binary_expr":
      const left = getDateFiltersFromBinaryExpression(expr.left, engine)
      const right = getDateFiltersFromBinaryExpression(expr.right, engine)

      if (operatorsUsedInMerge.includes(expr.operator)) {
        const merge = mergeFilters(left.pop(), right.pop())
        filtersToProcess.push(merge)
      } else {
        filtersToProcess.push(...left)
        filtersToProcess.push(...right)
      }
      break

    case "function":
      filtersToProcess.push(...getDateFiltersFromFunction(expr, engine))
      break

    case "interval":
      period = getPeriod(expr, engine)
      break
    case "expr_list":
      expr.value.forEach((arg) => {
        filtersToProcess.push(
          ...getDateFiltersFromBinaryExpression(arg, engine)
        )
      })
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
      type: undefined,
      numberOfPeriods: period?.numberOfPeriods,
      period: period?.period,
      field: column,
    })
  }

  return filters
}

function getDateFiltersFromFunction(
  expr: any,
  engine: DatabaseOpt
): DateFilter[] {
  const filters: DateFilter[] = []
  const filtersToProcess: DateFilter[] = []
  let column: string | undefined = ""
  let periodObj: PeriodObj | undefined
  const functionName = getNestedValue(expr, paths[engine].function.name)

  switch (functionName) {
    case "YEAR":
    case "MONTH":
      periodObj = {
        period: getPeriodFormatted(functionName.toLowerCase()),
        numberOfPeriods: 1,
      }
      break
    case "DATEADD":
      const period = getPeriodFormatted(
        getNestedValue(
          expr,
          paths[engine].functions[functionName.toLowerCase()].period
        ).toLowerCase()
      )
      const numberOfPeriods = getNestedValue(
        expr,
        paths[engine].functions[functionName.toLowerCase()].interval
      )

      periodObj = {
        period,
        numberOfPeriods,
      }
      break
  }

  expr.args?.value.forEach((arg) => {
    switch (arg.type) {
      case "binary_expr":
        filtersToProcess.push(
          ...getDateFiltersFromBinaryExpression(arg, engine)
        )
        break

      case "function":
        filtersToProcess.push(...getDateFiltersFromFunction(arg, engine))
        break

      case "column_ref":
        // required for bigQuery TIMESTAMP_TRUNC because MONTH/DAY/YEAR is a column_ref for some reason
        if (!column) {
          column = getColumnName(arg, engine)
        }
        break

      case "single_quote_string":
      case "interval":
        periodObj = getPeriod(arg, engine)
        break
      case "string":
        if (functionName === "PARSE_TIMESTAMP") {
          const timestamp = arg.value
          const lastLetter = timestamp.charAt(timestamp.length - 1)
          periodObj = {
            period: getPeriodFormatted(lastLetter),
            numberOfPeriods: 1,
          }
        }
    }
  })

  filtersToProcess.forEach((filter) => {
    if (!filter.field) {
      filter.field = column
    }
    if (!filter.period) {
      filter.period = periodObj?.period
    }
    if (!filter.numberOfPeriods) {
      filter.numberOfPeriods = periodObj?.numberOfPeriods
    }
    filters.push(filter)
  })
  if (filters.length === 0 && (column || periodObj)) {
    filters.push({
      type: undefined,
      numberOfPeriods: periodObj?.numberOfPeriods,
      period: periodObj?.period,
      field: column,
    })
  }
  return filters
}

function getDateFiltersFromExtract(
  expr: any,
  engine: DatabaseOpt
): DateFilter[] {
  const filters: DateFilter[] = []
  const filtersToProcess: DateFilter[] = []
  let column: string | undefined = ""
  let period: Period

  switch (expr.args.source.type) {
    case "column_ref":
      column = getColumnName(expr, engine)
      break
    case "binary_expr":
      const binaryFilters = getDateFiltersFromBinaryExpression(
        expr.args.source,
        engine
      )
      filtersToProcess.push(...binaryFilters)
      break
    case "function":
      const functionFilters = getDateFiltersFromFunction(
        expr.args.source,
        engine
      )
      filtersToProcess.push(...functionFilters)
      break
    default:
      break
  }

  if (expr.args.field) {
    period = getPeriodFormatted(expr.args.field.toLowerCase())
  }
  filtersToProcess.forEach((filter) => {
    if (!filter.field) {
      filter.field = column
    }
    if (!filter.period) {
      filter.period = period
    }
    filters.push(filter)
  })
  if (filters.length === 0 && (column || period)) {
    filters.push({
      type: undefined,
      numberOfPeriods: undefined,
      period: period,
      field: column,
    })
  }
  return filters
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

function getPeriod(
  expr: any,
  engine: DatabaseOpt,
  unit?: string
): PeriodObj | undefined {
  if (expr.type === "interval") {
    return getPeriod(expr.expr, engine, expr.unit)
  }

  switch (expr.type) {
    case "number":
      return {
        period: getPeriodFormatted(unit!),
        numberOfPeriods: expr.value,
      }
    case "single_quote_string":
      const split = expr.value.split(" ")
      const numberOfPeriods = split.length > 1 ? parseInt(split[0]) : undefined
      const period = getPeriodFormatted(split.pop().toLowerCase())
      return { period, numberOfPeriods }
  }
}

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
  if (!expr.args?.value) return
  expr.args.value.forEach((arg) => {
    if (arg.type === "column_ref") {
      const functionName = getNestedValue(
        expr,
        paths[engine].function.name
      )?.toLowerCase()
      if (functionName === "dateadd") return
      return getNestedValue(
        expr,
        paths[engine].column_ref.functions[functionName]
      )
    }
  })
  return undefined
}

function getColumnNameFromExtract(expr: any, engine: DatabaseOpt) {
  if (
    expr.args.source.type === "column_ref" &&
    "extract" in paths[engine]?.column_ref
  ) {
    return getNestedValue(expr, paths[engine].column_ref.extract)
  }
  return undefined
}

function getPeriodFormatted(period: string): Period {
  switch (period) {
    case "day":
    case "days":
    case "D":
      return "days"
    case "week":
    case "weeks":
      return "weeks"
    case "month":
    case "months":
    case "M":
      return "months"
    case "year":
    case "years":
    case "Y":
      return "years"
    case "quarter":
    case "quarters":
      return "quarters"
  }
}

const sqlQuery = `SELECT *
FROM transactions
WHERE created_at >= date_trunc('month', CURRENT_DATE)
AND created_at < (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month')`

const filters = getDateFiltersFromSQLQuery({ sqlQuery, database: "postgresql" })
console.log(filters)
