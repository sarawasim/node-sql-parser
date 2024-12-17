// @ts-nocheck
import { DateFilter } from "../dateFilter.js"
import pkg from "node-sql-parser"
const { Parser, Binary, Function } = pkg

export function getSnowflakeFilters(sqlQuery: string): DateFilter[] {
  const parser = new Parser()
  const ast = parser.astify(sqlQuery, { database: "snowflake" })
  if (!ast || Array.isArray(ast)) return []
  const filters: DateFilter[] = []
  const where = ast.where
  if (!where) return []

  exploreExpressions(where, filters)

  return filters
}

function exploreExpressions(
  expression: Binary | Function | null,
  filters: DateFilter[]
) {
  if (!expression) return
  if (expression.type === "binary_expr") {
    exploreBinaryExpression(expression, filters)
  } else {
    exploreFunctionExpression(expression, filters)
  }
}

function exploreBinaryExpression(
  expression: Binary,
  filters: DateFilter[],
  columnName?: string,
  exprParent?: Binary
) {
  const expressionType = expression.type

  let columnRef = getColumnName(expression)
  let columnLeft = ""
  let columnRight = ""
  let extractResult = ""

  let exprIsDateFunction = false
  if (expressionType === "extract") {
    extractResult = exploreExtract(expression, filters, columnRef || columnName)
  } else if (expression.type === "binary_expr") {
    columnLeft =
      exploreBinaryExpression(
        expression.left,
        filters,
        columnRef || columnName,
        expression
      ) || columnName

    columnRight = exploreBinaryExpression(
      expression.right,
      filters,
      columnLeft || columnRef,
      expression
    )
  } else if (expressionType === "function") {
    exprIsDateFunction = exploreFunctionExpression(expression, filters)
  }

  if (exprIsDateFunction || extractResult) {
    let period = ""
    let filter = undefined
    if (typeof exprIsDateFunction === "string") {
      period = exprIsDateFunction
    } else if (typeof exprIsDateFunction === "boolean") {
      period = ""
    } else {
      filter = exprIsDateFunction
      columnRef = columnName
    }

    const dateFilter = filter || parseDateFilter(exprParent, period)
    if (dateFilter) {
      dateFilter.field = columnLeft || columnRight || columnRef || columnName
      filters.push(dateFilter)
    }
  }
}

function exploreFunctionExpression(expression: Function) {
  const functionName = expression.name.name[0].value.toLowerCase()
  if (functionName === "current_date") {
    return true
  } else if (functionName === "year" || functionName === "month") {
    return functionName
  } else if (functionName === "date_sub") {
    return getPeriodFormatted(expression.args?.value[1].unit)
  } else if (functionName === "date_trunc") {
    const period = expression.args.value[0].value
    let numOfPeriod = expression.args.value[1].value
    let type = ""
    if (numOfPeriod === 0) {
      type = "current"
    } else if (numOfPeriod < 0) {
      type = "previous"
      numOfPeriod *= -1
    } else type = "next"

    return {
      type,
      numberOfPeriods: numOfPeriod,
      period,
    }
  } else if (functionName === "dateadd") {
    const period = expression.args.value[0].column
    let numOfPeriod = expression.args.value[1].value
    let type = ""
    if (numOfPeriod === 0) {
      type = "current"
    } else if (numOfPeriod < 0) {
      type = "previous"
      numOfPeriod *= -1
    } else type = "next"

    return {
      type,
      numberOfPeriods: numOfPeriod,
      period,
    }
  }
}

function exploreExtract(expression, filters, columnName) {
  if (!expression) return
  let columnRef = getColumnName(expression)

  if (expression.args.source.type === "binary_expr") {
    exploreBinaryExpression(
      expression.args.source,
      filters,
      columnRef || columnName
    )
  } else if (expression.args.source.type === "function") {
    return exploreFunctionExpression(expression.args.source)
  }
  return columnRef
}

function parseDateFilter(exprParent, period): DateFilter | null {
  const type = getType(exprParent, exprParent.operator)
  const numberOfPeriods = getNumberOfPeriods(exprParent)
  period = period ? period : getPeriod(exprParent)
  period = getPeriodFormatted(period.toLowerCase())

  return {
    type,
    numberOfPeriods,
    period,
  }
}

function getType(expr: any, operator: string): string {
  const isCurrentDate = (side: any) =>
    side?.type === "function" &&
    side?.name?.name?.[0]?.value?.toLowerCase() === "current_date"

  const isDateFunction = (side: any) =>
    side?.type === "function" &&
    ["date_sub", "year", "month", "date_trunc"].includes(
      side?.name?.name?.[0]?.value?.toLowerCase()
    )

  const isExtract = (side: any) =>
    side?.type === "extract" &&
    ["year", "month"].includes(side?.args?.field?.toLowerCase())

  const getTypeByOperator = (operator: string, defaultType = ""): string => {
    switch (operator) {
      case "+":
        return "next"
      case "=":
        return "current"
      case "-":
        return "last"
      default:
        return defaultType
    }
  }

  if (isCurrentDate(expr.left) || isCurrentDate(expr.right)) {
    return getTypeByOperator(operator)
  }

  if (isDateFunction(expr.left) || isDateFunction(expr.right)) {
    return getTypeByOperator(operator, "current")
  }

  if (isExtract(expr.left) || isExtract(expr.right)) {
    return getTypeByOperator(operator, "current")
  }

  return ""
}

function getNumberOfPeriods(expr: any) {
  if (expr.type === "binary_expr") {
    if (expr.right.type === "interval") {
      const intervalNumber = expr.right.expr.value
      return parseInt(intervalNumber)
    }
  }
  if (expr.right.type === "function") {
    if (expr.right.name.name[0].value.toLowerCase() === "date_sub") {
      return expr.right.args.value[1].expr.value
    }
  }

  return 1
}

function getPeriod(expr: any) {
  if (expr.type === "binary_expr") {
    if (expr.right.type === "interval") {
      const intervalString = expr.right.expr.value.toLowerCase()
      return getPeriodFormatted(intervalString.split(" ")[1])
    }
    if (expr.right.type === "extract") {
      return expr.right.args.field
    }
    if (expr.left.type === "extract") {
      return expr.left.args.field
    }
  }
  if (
    expr.type === "function" &&
    expr.name.name[0].value.toLowerCase() === "date_trunc"
  ) {
    const intervalString = expr.args.value[0].value.toLowerCase()
    return getPeriodFormatted(intervalString)
  }
}

function getColumnName(expr: any) {
  if (expr.type === "binary_expr") {
    if (expr.left.type === "column_ref") {
      return expr.left.column
    }
    if (expr.right.type === "column_ref") {
      return expr.right.column
    }
  } else if (expr.type === "function") {
    if (expr.args?.value[0]?.type === "column_ref") {
      return expr.args?.value[0].column
    }
  } else if (expr.type === "column_ref") {
    return expr.column
  } else if (expr.type === "extract") {
    return expr.args?.source?.column
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
    case "year":
    case "years":
      return "years"
    case "quarter":
    case "quarters":
      return "quarters"
  }
}