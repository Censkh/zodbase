module.exports = {
  docs: [
    "getting-started",
    {
      type: "category",
      label: "Model your data",
      collapsed: false,
      items: ["tables-and-schemas", "indexes-and-relations"],
    },
    {
      type: "category",
      label: "Read and write",
      collapsed: false,
      items: ["queries", "mutations", "results-and-validation", "transactions"],
    },
    {
      type: "category",
      label: "Adaptors",
      link: {
        type: "doc",
        id: "adaptors",
      },
      items: [
        "adaptors/bun-sqlite",
        "adaptors/better-sqlite3",
        "adaptors/d1",
        "adaptors/postgres",
        "adaptors/mysql",
        "adaptors/cockroach",
        "adaptors/turso",
        "adaptors/expo-sqlite",
        "adaptors/sqlite",
      ],
    },
    {
      type: "category",
      label: "Integrate",
      collapsed: false,
      items: ["configuration", "rsql", "sql"],
    },
    "reference",
  ],
};
