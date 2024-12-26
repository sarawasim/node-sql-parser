export type Period = "days" | "weeks" | "months" | "quarters" | "years"

export type TimePeriod = {
  period: Period
  numberOfPeriods: number
}

export const operatorsUsedInMerge = [
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

export const timeMapper = {
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

export function getPeriodFormatted(period: string): Period {
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
