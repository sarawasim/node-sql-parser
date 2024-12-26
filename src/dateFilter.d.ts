export interface DateFilter {
  type: "current" | "last" | "next" | "previous"
  numberOfPeriods: number
  period: "days" | "weeks" | "months" | "quarters" | "years"
  field: string
  truncatedDate?: Date
}
