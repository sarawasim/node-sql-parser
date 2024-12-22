import { getDateFiltersFromSQLQuery } from "../../index.js"

import { expect } from "chai"
import { describe, it } from "node:test"

const database = "snowflake"

describe("Snowflake parser", () => {
  it("should return no date filters when query does not include a date condition", () => {
    const sqlQuery = "SELECT * FROM transactions WHERE id = 1"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(0)
  })

  it("should parse YEAR(transaction_date) = YEAR(CURRENT_DATE()) AND MONTH(transaction_date) = MONTH(CURRENT_DATE()) as a 'current' year filter and a 'current' month filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE()) AND MONTH(transaction_date) = MONTH(CURRENT_DATE())"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(2)
    // expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("years")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    // expect(dateFilters[1].type).to.be.eql("current")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("months")
    expect(dateFilters[1].field).to.be.eql("transaction_date")
  })

  it("should parse EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) as a 'current' year filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE())"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(1)
    // expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("years")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse YEAR(transaction_date) = YEAR(CURRENT_DATE()) AND MONTH(transaction_date) = MONTH(CURRENT_DATE()) as a 'current' year filter and a 'current' month filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE()) AND MONTH(transaction_date) = MONTH(CURRENT_DATE())"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(2)
    // expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("years")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    // expect(dateFilters[1].type).to.be.eql("current")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("months")
    expect(dateFilters[1].field).to.be.eql("transaction_date")
  })

  it("should parse EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) AND EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE()) as a 'current' year filter and a 'current' month filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) AND EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE())"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(2)
    // expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("years")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    // expect(dateFilters[1].type).to.be.eql("current")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("months")
    expect(dateFilters[1].field).to.be.eql("transaction_date")
  })

  it("should parse transaction_date >= CURRENT_DATE() - INTERVAL '1 YEAR' as a 'last' year filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE() - INTERVAL '1 YEAR'"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(1)
    // expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("years")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse transaction_date >= DATEADD(YEAR, -1, CURRENT_DATE()) as a 'last' year filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= DATEADD(YEAR, -1, CURRENT_DATE())"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(1)
    // expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("years")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse transaction_date >= CURRENT_DATE() - INTERVAL '90 DAY' as a 'last' 90 days filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE() - INTERVAL '90 DAY'"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(1)
    // expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(90)
    expect(dateFilters[0].period).to.be.eql("days")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse transaction_date >= CURRENT_DATE() - INTERVAL '6 MONTH' as a 'last' 6 months filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= CURRENT_DATE() - INTERVAL '6 MONTH'"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(1)
    // expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(6)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse transaction_date >= DATEADD(DAY, -90, CURRENT_DATE()) as a 'last' 90 days filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= DATEADD(DAY, -90, CURRENT_DATE())"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(1)
    // expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(90)
    expect(dateFilters[0].period).to.be.eql("days")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse transaction_date >= DATE_TRUNC('MONTH', CURRENT_DATE() - INTERVAL '1 MONTH') AND transaction_date < DATE_TRUNC('MONTH', CURRENT_DATE()) as a 'last' month filter and a 'previous' month filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= DATE_TRUNC('MONTH', CURRENT_DATE() - INTERVAL '1 MONTH') AND transaction_date < DATE_TRUNC('MONTH', CURRENT_DATE())"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(2)
    // expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    // expect(dateFilters[1].type).to.be.eql("previous")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("months")
    expect(dateFilters[1].field).to.be.eql("transaction_date")
  })

  it("should parse transaction_date >= DATEADD(month, -1, DATE_TRUNC('MONTH', CURRENT_DATE())) AND transaction_date < DATE_TRUNC('MONTH', CURRENT_DATE()) as a 'last' month filter and a 'previous' month filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= DATEADD(month, -1, DATE_TRUNC('MONTH', CURRENT_DATE())) AND transaction_date < DATE_TRUNC('MONTH', CURRENT_DATE())"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(2)
    // expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    // expect(dateFilters[1].type).to.be.eql("previous")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("months")
    expect(dateFilters[1].field).to.be.eql("transaction_date")
  })

  it("should parse transaction_date >= DATEADD(month, -1, DATE_TRUNC('MONTH', CURRENT_DATE())) AND transaction_date < DATE_TRUNC('MONTH', CURRENT_DATE()) as a 'last' month filter and a 'previous' month filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date BETWEEN DATE_TRUNC('MONTH', CURRENT_DATE() - INTERVAL '1 MONTH') AND LAST_DAY(DATE_TRUNC('MONTH', CURRENT_DATE() - INTERVAL '1 MONTH'))"

    const dateFilters = getDateFiltersFromSQLQuery({
      sqlQuery,
      database,
    })

    expect(dateFilters).to.have.length(2)
    // expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    // expect(dateFilters[1].type).to.be.eql("previous")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("months")
    expect(dateFilters[1].field).to.be.eql("transaction_date")
  })
})
