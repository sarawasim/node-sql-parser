// @ts-nocheck
import { DateFilter } from "../dateFilter.js"
import pkg from "node-sql-parser"
const { Parser, Binary, Function } = pkg

export function getMysqlFilters(sqlQuery: string): DateFilter[] {
  const parser = new Parser()
  const ast = parser.astify(sqlQuery, { database: "mysql" })
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

  let exprIsDateFunction = false
  if (expressionType === "extract") {
    return exploreExtract(expression, filters, columnRef || columnName)
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

  if (exprIsDateFunction) {
    const period =
      typeof exprIsDateFunction === "string" ? exprIsDateFunction : undefined
    const dateFilter = parseDateFilter(exprParent, period)
    if (dateFilter) {
      dateFilter.field = columnLeft || columnRight || columnRef || columnName
      filters.push(dateFilter)
    }
  }
}

function exploreFunctionExpression(
  expression: Function,
  filters: DateFilter[]
) {
  const functionName = expression.name.name[0].value.toLowerCase()
  if (functionName === "curdate" || functionName === "now") {
    return true
  } else if (functionName === "year" || functionName === "month") {
    return functionName
  } else if (functionName === "date_sub") {
    return getPeriodFormatted(expression.args?.value[1].unit)
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
  }
  return columnRef
}

function parseDateFilter(exprParent, period): DateFilter | null {
  const type = getType(exprParent, exprParent.operator)
  const numberOfPeriods = getNumberOfPeriods(exprParent)
  period = period ? period : getPeriod(exprParent)

  return {
    type,
    numberOfPeriods,
    period,
  }
}

function getType(expr: any, operator: string): string {
  const isCurDate = (side: any) =>
    side?.type === "function" &&
    (side?.name?.name?.[0]?.value?.toLowerCase() === "curdate" ||
      side?.name?.name?.[0]?.value?.toLowerCase() === "now")

  const isDateFunction = (side: any) =>
    side?.type === "function" &&
    ["date_sub", "year", "month"].includes(
      side?.name?.name?.[0]?.value?.toLowerCase()
    )

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

  if (isCurDate(expr.left) || isCurDate(expr.right)) {
    return getTypeByOperator(operator)
  }

  if (isDateFunction(expr.left) || isDateFunction(expr.right)) {
    return getTypeByOperator(operator, "current")
  }

  return ""
}

function getNumberOfPeriods(expr: any) {
  if (expr.type === "binary_expr" && expr.right.type === "interval") {
    const intervalNumber = expr.right.expr.value
    return parseInt(intervalNumber)
  }
  if (
    expr.right.type === "function" &&
    expr.right.name.name[0].value.toLowerCase() === "date_sub"
  ) {
    return expr.right.args.value[1].expr.value
  }
  return 1
}

function getPeriod(expr: any) {
  if (expr.type === "binary_expr") {
    if (expr.right.type === "interval") {
      const intervalString = expr.right.unit.toLowerCase()
      return getPeriodFormatted(intervalString)
    }
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
    case "quarter":
    case "quarters":
      return "quarters"
  }
}
