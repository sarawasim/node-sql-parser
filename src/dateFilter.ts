export interface DateFilter {
  type: "current" | "last" | "next" | "previous" | undefined
  numberOfPeriods: number | undefined
  period: "days" | "weeks" | "months" | "quarters" | "years" | undefined
  field: string | undefined
}
