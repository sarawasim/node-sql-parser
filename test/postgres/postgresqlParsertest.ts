import { getPostgresqlFilters } from "../../src/postgres/postgresqlParser.js"

import { expect } from "chai"
import { describe, it } from "node:test"

describe("Postgresql parser", () => {
  it("should return no date filters when query does not include a date condition", () => {
    const sqlQuery = "SELECT * FROM transactions WHERE id = 1"

    const dateFilters = getPostgresqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(0)
  })

  it("should parse DATE_TRUNC('month') with CURRENT_DATE as two 'current' month filters", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE)"

    const dateFilters = getPostgresqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(2)
    expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("month")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    expect(dateFilters[1].type).to.be.eql("current")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("month")
    expect(dateFilters[1].field).to.be.undefined
  })

  it("should parse date range using date_trunc('month') and CURRENT_DATE with INTERVAL '1 month'", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE created_at >= date_trunc('month', CURRENT_DATE) AND created_at < (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month')"

    const dateFilters = getPostgresqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(2)
    expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("month")
    expect(dateFilters[0].field).to.be.eql("created_at")
    expect(dateFilters[1].type).to.be.eql("next")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("month")
    expect(dateFilters[1].field).to.be.eql("created_at")
  })

  it("should parse CURRENT_DATE - INTERVAL '1 month' as a 'last' filter for 1 month", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE - INTERVAL '1 month'"

    const dateFilters = getPostgresqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(1)
    expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse CURRENT_DATE - INTERVAL '30 months' as a 'last' filter for 30 months", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 months'"

    const dateFilters = getPostgresqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(1)
    expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(30)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse DATE_TRUNC('month') with CURRENT_DATE - INTERVAL '1 month' as two 'current' month filters", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')"

    const dateFilters = getPostgresqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(2)
    expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("month")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    expect(dateFilters[1].type).to.be.eql("current")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("month")
    expect(dateFilters[1].field).to.be.undefined
  })

  it("should parse DATE_TRUNC('quarter') with CURRENT_DATE - INTERVAL '1 quarter' as two 'current' quarter filters", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE DATE_TRUNC('quarter', transaction_date) = DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '1 quarter')"

    const dateFilters = getPostgresqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(2)
    expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("quarter")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    expect(dateFilters[1].type).to.be.eql("current")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("quarter")
    expect(dateFilters[1].field).to.be.undefined
  })

  it("should parse EXTRACT with CURRENT_DATE - INTERVAL '1 month' as two 'last' month filters", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month') AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')"

    const dateFilters = getPostgresqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(2)
    expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    expect(dateFilters[1].type).to.be.eql("last")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("months")
    expect(dateFilters[1].field).to.be.eql("transaction_date")
  })
})
