import { getMysqlFilters } from "../../src/mysql/mysqlParser.js"

import { expect } from "chai"
import { describe, it } from "node:test"

describe("Mysql parser", () => {
  it("should return no date filters when query does not include a date condition", () => {
    const sqlQuery = "SELECT * FROM transactions WHERE id = 1"

    const dateFilters = getMysqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(0)
  })

  // it("should parse YEAR(transaction_date) = YEAR(CURRENT_DATE()) AND MONTH(transaction_date) = MONTH(CURRENT_DATE()) as a 'current' date filter", () => {
  //   const sqlQuery =
  //     "SELECT * FROM transactions WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE()) AND MONTH(transaction_date) = MONTH(CURRENT_DATE())"

  //   const dateFilters = getMysqlFilters(sqlQuery)

  //   expect(dateFilters).to.have.length(2)
  //   expect(dateFilters[0].type).to.be.eql("current")
  //   expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
  //   expect(dateFilters[0].period).to.be.eql("years")
  //   expect(dateFilters[0].field).to.be.eql("transaction_date")
  //   expect(dateFilters[1].type).to.be.eql("current")
  //   expect(dateFilters[1].numberOfPeriods).to.be.eql(1)
  //   expect(dateFilters[1].period).to.be.eql("months")
  //   expect(dateFilters[1].field).to.be.eql("transaction_date")
  // })

  // it("should parse YEAR(transaction_date) = YEAR(CURDATE()) as a 'current' date filter", () => {
  //   const sqlQuery =
  //     "SELECT * FROM transactions WHERE YEAR(transaction_date) = YEAR(CURDATE())"

  //   const dateFilters = getMysqlFilters(sqlQuery)

  //   expect(dateFilters).to.have.length(2)
  //   expect(dateFilters[0].type).to.be.eql("current")
  //   expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
  //   expect(dateFilters[0].period).to.be.eql("years")
  //   expect(dateFilters[0].field).to.be.eql("transaction_date")
  // })

  it("should parse CURDATE() - INTERVAL 1 MONTH as a 'last' date filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= CURDATE() - INTERVAL 1 MONTH"

    const dateFilters = getMysqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(1)
    expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse DATE_SUB(CURDATE(), INTERVAL 1 MONTH) as a 'current' date filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)"

    const dateFilters = getMysqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(1)
    expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse DATE_SUB(NOW(), INTERVAL 1 MONTH) as a 'current' date filter", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(NOW(), INTERVAL 1 MONTH)"

    const dateFilters = getMysqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(1)
    expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(1)
    expect(dateFilters[0].period).to.be.eql("months")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse DATE_SUB(CURDATE(), INTERVAL 90 DAY) as a 'current' date filter with 90 days", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)"

    const dateFilters = getMysqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(1)
    expect(dateFilters[0].type).to.be.eql("current")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(90)
    expect(dateFilters[0].period).to.be.eql("days")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })

  it("should parse CURDATE() - INTERVAL 90 DAY as a 'last' date filter with 90 days", () => {
    const sqlQuery =
      "SELECT * FROM transactions WHERE transaction_date >= CURDATE() - INTERVAL 90 DAY"

    const dateFilters = getMysqlFilters(sqlQuery)

    expect(dateFilters).to.have.length(1)
    expect(dateFilters[0].type).to.be.eql("last")
    expect(dateFilters[0].numberOfPeriods).to.be.eql(90)
    expect(dateFilters[0].period).to.be.eql("days")
    expect(dateFilters[0].field).to.be.eql("transaction_date")
  })
})
