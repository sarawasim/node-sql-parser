import { DateFilter } from "../dateFilter.js"
import pkg from "node-sql-parser"
const { Parser } = pkg

interface BinaryExpr {
  type: "binary_expr"
  operator: string
  left: any
  right: any
}

interface FunctionExpr {
  type: "function"
  name: any
  args?: any
}

export function getPostgresqlFilters(sqlQuery: string): DateFilter[] {
  const parser = new Parser()
  const ast = parser.astify(sqlQuery, { database: "postgresql" })
  if (!ast || Array.isArray(ast)) return []
  const filters: DateFilter[] = []
  // @ts-ignore
  const where = ast.where
  if (!where) return []

  exploreExpressions(where, filters)

  return filters
}

function exploreExpressions(
  expression: BinaryExpr | FunctionExpr | null,
  filters: DateFilter[]
) {
  if (!expression) return
  if (expression.type === "binary_expr") {
    exploreBinaryExpression(expression, filters)
  } else {
    exploreFunctionExpression(expression)
  }
}

function exploreBinaryExpression(
  expression: BinaryExpr | FunctionExpr,
  filters: DateFilter[],
  columnName?: string,
  exprParent?: BinaryExpr
) {
  const expressionType = expression.type
  let columnRef: string | undefined = getColumnName(expression)
  let columnLeft: string | undefined = ""
  let columnRight: string | undefined = ""

  let exprIsDateFunction = false
  // @ts-ignore
  if (expressionType === "extract") {
    return exploreExtract(expression, filters, columnRef || columnName)
  } else if (expressionType === "binary_expr") {
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
    exprIsDateFunction = exploreFunctionExpression(expression)
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

function exploreFunctionExpression(expression: FunctionExpr) {
  if (expression.name.name[0].value.toLowerCase() === "current_date") {
    return true
  } else if (expression.name.name[0].value.toLowerCase() === "date_trunc") {
    return expression.args?.value[0].value
  }
}

function exploreExtract(
  expression: any,
  filters: DateFilter[],
  columnName: string
) {
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

function parseDateFilter(
  exprParent: BinaryExpr,
  period: string
): DateFilter | null {
  const type = getType(exprParent, exprParent.operator)
  const numberOfPeriods = getNumberOfPeriods(exprParent)
  period = period ? period : getPeriod(exprParent)

  return {
    type,
    numberOfPeriods,
    // @ts-ignore
    period,
  }
}

function getType(expr: any, operator: string): any {
  const isCurrentDate = (side: any) =>
    side.type === "function" &&
    side.name.name[0].value.toLowerCase() === "current_date"

  const isDateTrunc = (side: any) =>
    side.type === "function" &&
    side.name.name[0].value.toLowerCase() === "date_trunc"

  const getTypeByOperator = (
    operator: string,
    isDateTrunc: boolean
  ): string => {
    switch (operator) {
      case "+":
        return "next"
      case "=":
        return "current"
      case "-":
        return "last"
      default:
        return isDateTrunc ? "current" : ""
    }
  }

  if (isCurrentDate(expr.left) || isCurrentDate(expr.right)) {
    return getTypeByOperator(operator, false)
  }

  if (isDateTrunc(expr.left) || isDateTrunc(expr.right)) {
    return getTypeByOperator(operator, true)
  }

  return ""
}

function getNumberOfPeriods(expr: any) {
  if (expr.type === "binary_expr" && expr.right.type === "interval") {
    const intervalNumber = expr.right.expr.value
    return parseInt(intervalNumber)
  }
  return 1
}

function getPeriod(expr: any): "days" | "weeks" | "months" | "quarters" {
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

function getColumnName(expr: any) {
  if (expr.type === "binary_expr") {
    if (expr.left.type === "column_ref") {
      return expr.left.column.expr.value
    }
    if (expr.right.type === "column_ref") {
      return expr.right.column.expr.value
    }
  } else if (expr.type === "function") {
    if (expr.args?.value[1].type === "column_ref") {
      return expr.args.value[1].column.expr.value
    }
  } else if (expr.type === "column_ref") {
    return expr.column.expr.value
  } else if (expr.type === "extract") {
    return expr.args?.source?.column?.expr.value
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
