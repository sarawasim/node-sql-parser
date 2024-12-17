import { getRedshiftFilters } from "../../src/redshift/redshiftParser.js"

import { expect } from "chai"
import { describe, it } from "node:test"

describe("Redshift parser", () => {
  it("should return no date filters when query does not include a date condition", () => {
    const sqlQuery = "SELECT * FROM transactions WHERE id = 1"

    const dateFilters = getRedshiftFilters(sqlQuery)

    expect(dateFilters).to.have.length(0)
  })

  //   it("should parse EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE) AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE) as two 'current' filters", () => {
  //     const sqlQuery =
  //       "SELECT * FROM transactions WHERE EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE) AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE)"

  //     const dateFilters = getRedshiftFilters(sqlQuery)

  //     expect(dateFilters).to.have.length(2)
  //     expect(dateFilters[0].type).to.be.eql("current")
  //     expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
  //     expect(dateFilters[0].period).to.be.eql("months")
  //     expect(dateFilters[0].field).to.be.eql("transaction_date")
  //     expect(dateFilters[1].type).to.be.eql("current")
  //     expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
  //     expect(dateFilters[1].period).to.be.eql("years")
  //     expect(dateFilters[1].field).to.be.undefined
  //   })

  it("should parse date range using transaction_date >= DATE_TRUNC('month', CURRENT_DATE) AND transaction_date < (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month') as two month filters", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= DATE_TRUNC('month', CURRENT_DATE) AND transaction_date < (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month')"

    const dateFilters = getRedshiftFilters(sqlQuery)

    expect(dateFilters).to.have.length(2)
    expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    expect(dateFilters[1].type).to.be.eql("next")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("months")
    expect(dateFilters[1].field).to.be.eql("transaction_date")
  })

  //   it("transaction_date >= DATEADD(day, -90, GETDATE()) as a 'last' filter for 90 days", () => {
  //     const sqlQuery =
  //       "SELECT * FROM transactions WHERE transaction_date >= DATEADD(day, -90, GETDATE())"

  //     const dateFilters = getRedshiftFilters(sqlQuery)

  //     expect(dateFilters).to.have.length(1)
  //     expect(dateFilters[0].type).to.be.eql("last")
  //     expect(dateFilters[0].numberOfPeriods).to.be.eql(90)
  //     expect(dateFilters[0].period).to.be.eql("days")
  //     expect(dateFilters[0].field).to.be.eql("transaction_date")
  //   })

  //   it("should parse transaction_date >= DATEADD(day, -90, CURRENT_DATE) as a 'last' filter for 90 days", () => {
  //     const sqlQuery =
  //       "SELECT * FROM transactions WHERE transaction_date >= DATEADD(day, -90, CURRENT_DATE)"

  //     const dateFilters = getRedshiftFilters(sqlQuery)

  //     expect(dateFilters).to.have.length(1)
  //     expect(dateFilters[0].type).to.be.eql("last")
  //     expect(dateFilters[0].numberOfPeriods).to.be.eql(90)
  //     expect(dateFilters[0].period).to.be.eql("days")
  //     expect(dateFilters[0].field).to.be.eql("transaction_date")
  //   })

  it("should parse transaction_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND transaction_date < DATE_TRUNC('month', CURRENT_DATE) as two month filters", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND transaction_date < DATE_TRUNC('month', CURRENT_DATE)"

    const dateFilters = getRedshiftFilters(sqlQuery)

    expect(dateFilters).to.have.length(2)
    expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    expect(dateFilters[1].type).to.be.eql("current")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })
})
