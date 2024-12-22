import { getDateFiltersFromSQLQuery } from "../../index.js"

import { expect } from "chai"
import { describe, it } from "node:test"

const database = "postgresql"

describe("Postgresql parser", () => {
  it("should return no date filters when query does not include a date condition", () => {
    const sqlQuery = "SELECT * FROM transactions WHERE id = 1"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(0)
  })

  it("should parse DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE) as a 'current' 1 month filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE)"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(1)
    // expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse created_at >= date_trunc('month', CURRENT_DATE) AND created_at < (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month') as a two 'current' month filters", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE created_at >= date_trunc('month', CURRENT_DATE) AND created_at < (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month')"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(2)
    // expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("created_at")
    // expect(dateFilters[1].type).to.be.eql("current")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("months")
    expect(dateFilters[1].field).to.be.eql("created_at")
  })

  it("should parse transaction_date >= CURRENT_DATE - INTERVAL '1 month' as a 'last' 1 month filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE - INTERVAL '1 month'"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(1)
    // expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse transaction_date >= CURRENT_DATE - INTERVAL '30 months' as a 'last' 30 months filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 months'"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(1)
    // expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(30)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse DATE_TRUNC('month') with CURRENT_DATE - INTERVAL '1 month' as a 'previous' month filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE DATE_TRUNC('month', transaction_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(1)
    // expect(dateFilters[0].type).to.be.eql("previous")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse DATE_TRUNC('quarter') with CURRENT_DATE - INTERVAL '1 quarter' as a 'previous' quarter filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE DATE_TRUNC('quarter', transaction_date) = DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '1 quarter')"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(1)
    // expect(dateFilters[0].type).to.be.eql("previous")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("quarters")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month') AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month') as 1 'previous' month filter and one previous year filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE - INTERVAL '1 month') AND EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '1 month')"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(2)
    // expect(dateFilters[0].type).to.be.eql("previous")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    // expect(dateFilters[1].type).to.be.eql("previous")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("years")
    expect(dateFilters[1].field).to.be.eql("transaction_date")
  })
})
