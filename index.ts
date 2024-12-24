import pkg from "node-sql-parser"
const { Parser } = pkg

export interface DateFilter {
  type: "current" | "last" | "next" | "previous"
  numberOfPeriods: number
  period: "days" | "weeks" | "months" | "quarters" | "years"
  field: string
  truncatedDate?: Date
}

type DatabaseOpt =
  | "postgresql"
  | "mysql"
  | "snowflake"
  | "redshift"
  | "bigquery"

type Period = "days" | "weeks" | "months" | "quarters" | "years"

type PeriodObj = {
  period: Period
  numberOfPeriods: number
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

const now = new Date()

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
      delete filter.truncatedDate
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
  if (!obj || !path) return undefined
  try {
    return path.split(".").reduce((acc, key) => {
      if (key.includes("[") && key.includes("]")) {
        const [baseKey, index] = key.split("[")
        const arrayIndex = parseInt(index.split("]")[0])
        acc = acc ? acc[baseKey] : undefined
        return acc ? acc[arrayIndex] : undefined
      } else {
        return acc ? acc[key] : undefined
      }
    }, obj)
  } catch (error) {
    console.error(`Error getting nested value at path: ${path}`, error)
    return undefined
  }
}

function mergeFilters(
  filter1: DateFilter,
  filter2: DateFilter,
  operator: string
) {
  const refinedPeriod = getRefinedPeriod(filter1, filter2)
  const type: "previous" | "last" | "current" | "next" = processType(
    filter1,
    filter2,
    operator
  )
  return {
    truncatedDate: filter1?.truncatedDate || filter2?.truncatedDate,
    type: type,
    numberOfPeriods: refinedPeriod.numberOfPeriods,
    period: refinedPeriod.period,
    field: filter1?.field || filter2?.field,
  }
}

function getRefinedPeriod(filter1, filter2) {
  if (!filter1 && !filter2) return {}
  if (!filter1) return filter2
  if (!filter2) return filter1

  const period1 = filter1.period
  const period2 = filter2.period
  const numPeriods1 = filter1.numberOfPeriods
  const numPeriods2 = filter2.numberOfPeriods

  if (!period1) {
    return {
      period: period2,
      numberOfPeriods: numPeriods2 || numPeriods1,
    }
  }
  if (!period2) {
    return { period: period1, numberOfPeriods: numPeriods1 || numPeriods2 }
  }
  const duration1 = (numPeriods1 || 1) * timeMapper[period1]
  const duration2 = (numPeriods2 || 1) * timeMapper[period2]

  if (duration1 < duration2) {
    return { period: period1, numberOfPeriods: numPeriods1 }
  } else if (duration2 < duration1) {
    return { period: period2, numberOfPeriods: numPeriods2 }
  } else {
    return numPeriods2 < numPeriods1
      ? { period: period2, numberOfPeriods: numPeriods2 }
      : { period: period1, numberOfPeriods: numPeriods1 }
  }
}

function getDateFiltersFromBinaryExpression(
  expr: any,
  engine: DatabaseOpt
): DateFilter[] {
  const column: string | undefined = getColumnName(expr, engine)
  const filters: DateFilter[] = []
  const filtersToProcess: DateFilter[] = []

  let periodObj: PeriodObj | undefined
  let intervalTime = undefined

  switch (expr.type) {
    case "extract":
      filtersToProcess.push(...getDateFiltersFromExtract(expr, engine))
      break

    case "binary_expr":
      filtersToProcess.push(...processBinaryExpression(expr, engine))

      break

    case "function":
      filtersToProcess.push(...getDateFiltersFromFunction(expr, engine))
      break

    case "interval":
      periodObj = getPeriod(expr, engine)
      intervalTime = new Date(now)

      switch (periodObj.period) {
        case "months":
          if (expr.operator === "+") {
            intervalTime.setMonth(
              intervalTime.getMonth() + (periodObj.numberOfPeriods || 0)
            )
          } else {
            intervalTime.setMonth(
              intervalTime.getMonth() - (periodObj.numberOfPeriods || 0)
            )
          }
        case "days":
          if (expr.operator === "+") {
            intervalTime.setDate(
              intervalTime.getDate() + (periodObj.numberOfPeriods || 0)
            )
          } else {
            intervalTime.setDate(
              intervalTime.getDate() - (periodObj.numberOfPeriods || 0)
            )
          }
        case "weeks":
          if (expr.operator === "+") {
            intervalTime.setDate(
              intervalTime.getDate() + 7 * (periodObj.numberOfPeriods || 0)
            )
          } else {
            intervalTime.setDate(
              intervalTime.getDate() - 7 * (periodObj.numberOfPeriods || 0)
            )
          }
        case "years":
          if (expr.operator === "+") {
            intervalTime.setFullYear(
              intervalTime.getFullYear() + (periodObj.numberOfPeriods || 0)
            )
          } else {
            intervalTime.setFullYear(
              intervalTime.getFullYear() - (periodObj.numberOfPeriods || 0)
            )
          }
        case "quarters":
          if (expr.operator === "+") {
            intervalTime.setMonth(
              intervalTime.getMonth() + 3 * (periodObj.numberOfPeriods || 0)
            )
          } else {
            intervalTime.setMonth(
              intervalTime.getMonth() - 3 * (periodObj.numberOfPeriods || 0)
            )
          }
      }
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
      filter.period = periodObj?.period
    }
    if (!filter.numberOfPeriods) {
      filter.numberOfPeriods = periodObj?.numberOfPeriods
    }
    if (intervalTime) {
      filter.truncatedDate = intervalTime
    }
    filters.push(filter)
  })
  if (filters.length === 0 && (column || periodObj)) {
    filters.push({
      type: undefined,
      numberOfPeriods: periodObj?.numberOfPeriods,
      period: periodObj?.period,
      truncatedDate: intervalTime,
      field: column,
    })
  }

  return filters
}

function processType(
  dateFilter1: DateFilter,
  dateFilter2: DateFilter,
  operator: string
) {
  if (!dateFilter1 || !dateFilter2) return undefined
  let actualTime1 = dateFilter1.truncatedDate
  let actualTime2 = dateFilter2.truncatedDate

  if (!actualTime1 && !actualTime2) return
  if (dateFilter1.period && actualTime1) {
    actualTime1 = truncateDate(actualTime1, dateFilter1.period)
  }
  if (dateFilter2.period && actualTime2) {
    actualTime2 = truncateDate(actualTime2, dateFilter2.period)
  }
  switch (operator) {
    case "<":
    case "<=":
      if (!actualTime1) {
        return "previous"
      }
      if (!actualTime2) {
        return "last"
      }
      if (actualTime1.getTime() === actualTime2.getTime()) {
        return "last"
      } else if (actualTime1.getTime() < actualTime2.getTime()) {
        return "previous"
      }
      break
    case "=":
    case "BETWEEN":
      if (actualTime1?.getTime() == actualTime2?.getTime()) {
        return "current"
      } else if (
        dateFilter1.field &&
        actualTime1?.getTime() > actualTime2?.getTime()
      ) {
        return "previous"
      } else if (
        dateFilter2.field &&
        actualTime2?.getTime() > actualTime1?.getTime()
      ) {
        return "previous"
      } else if (
        dateFilter1.field &&
        actualTime1?.getTime() < actualTime2?.getTime()
      ) {
        return "next"
      } else if (
        dateFilter2.field &&
        actualTime2?.getTime() < actualTime1?.getTime()
      ) {
        return "next"
      }
      break
    case ">":
    case ">=":
      if (!actualTime1) {
        return "last"
      }
      if (!actualTime2) {
        return "next"
      }
      if (actualTime1.getTime() >= actualTime2.getTime()) {
        return "next"
      }
      break
    default:
      return undefined
  }
}

function processBinaryExpression(expr: any, engine: DatabaseOpt): DateFilter[] {
  const leftFilters = getDateFiltersFromBinaryExpression(expr.left, engine)
  const rightFilters = getDateFiltersFromBinaryExpression(expr.right, engine)

  if (operatorsUsedInMerge.includes(expr.operator)) {
    const merged = mergeFilters(
      leftFilters.pop(),
      rightFilters.pop(),
      expr.operator
    )
    return [merged]
  }

  return [...leftFilters, ...rightFilters]
}

function truncateDate(date: Date, period: string) {
  date.setHours(0, 0, 0, 0)
  switch (period) {
    case "days":
      return new Date(date.getFullYear(), date.getMonth(), date.getDate())
    case "weeks":
      let dayOfWeek = date.getDay()
      return new Date(
        date.getFullYear(),
        date.getMonth(),
        date.getDate() - dayOfWeek
      )
    case "months":
      return new Date(date.getFullYear(), date.getMonth(), 1)
    case "quarters":
      let quarterStartMonth = Math.floor(date.getMonth() / 3) * 3
      return new Date(date.getFullYear(), quarterStartMonth, 1)
    case "years":
      return new Date(date.getFullYear(), 1, 1)
  }
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
        filtersToProcess.push(...processBinaryExpression(arg, engine))
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

  let truncatedDate: Date = undefined

  if (functionName?.toLowerCase().includes("trunc")) {
    truncatedDate = truncateDate(new Date(now), periodObj?.period)
  }

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
    if (!filter.truncatedDate) {
      filter.truncatedDate = truncatedDate
    } else {
      if (functionName?.toLowerCase().includes("trunc")) {
        truncatedDate = truncateDate(
          new Date(filter.truncatedDate),
          periodObj?.period
        )
      }
      filter.truncatedDate = truncatedDate
    }
    filters.push(filter)
  })

  if (filters.length === 0 && (column || periodObj)) {
    filters.push({
      truncatedDate,
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
      const binaryFilters = processBinaryExpression(expr.args.source, engine)
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

const sqlQuery = `SELECT * FROM transactions WHERE TIMESTAMP_TRUNC(dateField, MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)`

const filters = getDateFiltersFromSQLQuery({ sqlQuery, database: "bigquery" })
console.log(filters)
