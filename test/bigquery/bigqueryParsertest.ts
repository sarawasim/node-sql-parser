import { getBigQueryFilters } from "../../src/bigquery/bigqueryParser.js"

import { expect } from "chai"
import { describe, it } from "node:test"

describe("Bigquery parser", () => {
  it("should return no date filters when query does not include a date condition", () => {
    const sqlQuery = "SELECT * FROM transactions WHERE id = 1"

    const dateFilters = getBigQueryFilters(sqlQuery)

    expect(dateFilters).to.have.length(0)
  })

  it("should parse EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) AND EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE()) as two 'current' filters", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) AND EXTRACT(MONTH FROM transaction_date) = EXTRACT(MONTH FROM CURRENT_DATE())"

    const dateFilters = getBigQueryFilters(sqlQuery)

    expect(dateFilters).to.have.length(2)
    expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("years")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
    expect(dateFilters[1].type).to.be.eql("current")
    expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[1].period).to.be.eql("months")
    expect(dateFilters[1].field).to.be.eql("transaction_date")
  })

  it("should parse date range using transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) as 'last' month filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)"

    const dateFilters = getBigQueryFilters(sqlQuery)

    expect(dateFilters).to.have.length(1)
    expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) as 'last' filter for 90 days ", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)"

    const dateFilters = getBigQueryFilters(sqlQuery)

    expect(dateFilters).to.have.length(1)
    expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(90)
    expect(dateFilters[0].period).to.be.eql("days")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) as a 'last' filter for 1 months", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)"

    const dateFilters = getBigQueryFilters(sqlQuery)

    expect(dateFilters).to.have.length(1)
    expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("years")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  // it('should parse PARSE_TIMESTAMP("%Y-%M-%D", transaction_date) = PARSE_TIMESTAMP("%Y-%M-%D", CURRENT_DATE()) as a \'previous\' filter', () => {
  //   const sqlQuery =
  //     'SELECT * FROM transactions WHERE PARSE_TIMESTAMP("%Y-%M-%D", transaction_date) = PARSE_TIMESTAMP("%Y-%M-%D", CURRENT_DATE())'

  //   const dateFilters = getBigQueryFilters(sqlQuery)

  //   expect(dateFilters).to.have.length(1)
  //   expect(dateFilters[0].type).to.be.eql("previous")
  //   expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
  //   expect(dateFilters[0].period).to.be.eql("months")
  //   expect(dateFilters[0].field).to.be.eql("transaction_date")
  // })

  // it("should parse TIMESTAMP_TRUNC(dateField, INTERVAL MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH) as a 'previous' month filter", () => {
  //   const sqlQuery =
  //     "SELECT * FROM transactions WHERE TIMESTAMP_TRUNC(dateField, INTERVAL MONTH) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 30 DAY), MONTH)"

  //   const dateFilters = getBigQueryFilters(sqlQuery)

  //   expect(dateFilters).to.have.length(1)
  //   expect(dateFilters[0].type).to.be.eql("previous")
  //   expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
  //   expect(dateFilters[0].period).to.be.eql("months")
  //   expect(dateFilters[0].field).to.be.eql("transaction_date")
  // })
})
