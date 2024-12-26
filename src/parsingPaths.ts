export const paths = {
  redshift: {
    column_ref: {
      column: "column.expr.value",
      functions: {},
      extract: "args.source.column.expr.value",
    },
    function: {
      name: "name.name[0].value",
    },
    functions: {
      dateadd: {
        period: "args.value[0].column.expr.value",
        interval: "args.value[1].value",
      },
    },
  },
  snowflake: {
    column_ref: {
      column: "column",
      functions: {
        year: "args.value[1].column",
        month: "args.value[1].column",
      },
      extract: "args.source.column",
    },
    function: {
      name: "name.name[0].value",
    },
    functions: {
      dateadd: {
        period: "args.value[0].column",
        interval: "args.value[1].value",
      },
    },
  },
  bigquery: {
    column_ref: {
      column: "column",
      functions: {
        parse_timestamp: "args.value[1].column",
        timestamp_trunc: "args.value[0].column",
      },
      extract: "args.source.column",
    },
    function: {
      name: "name.schema.value",
    },
    functions: {},
  },
  mysql: {
    column_ref: {
      column: "column",
      functions: {
        year: "args.value[1].column",
        month: "args.value[1].column",
      },
    },
    function: {
      name: "name.name[0].value",
    },
    functions: {},
  },
  postgresql: {
    column_ref: {
      column: "column.expr.value",
      functions: {
        date_trunc: "args.value[1].column.expr.value",
      },
      extract: "args.source.column.expr.value",
    },
    function: {
      name: "name.name[0].value",
    },
    functions: {},
  },
}
