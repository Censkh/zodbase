const path = require("node:path");
const baseUrl = "/";
const guideUrl = "/queries/";
const codeTheme = {
  plain: { color: "rgba(255, 255, 255, 0.97)", backgroundColor: "#1d2032" },
  styles: [
    { types: ["comment", "prolog", "doctype", "cdata"], style: { color: "rgba(255, 255, 255, 0.97)" } },
    { types: ["punctuation", "operator"], style: { color: "rgba(255, 255, 255, 0.97)" } },
    { types: ["keyword", "tag", "selector"], style: { color: "#c4b5fd", fontStyle: "normal" } },
    { types: ["string", "char", "attr-value", "regex"], style: { color: "#b6d6b0" } },
    { types: ["function", "class-name", "builtin"], style: { color: "#8bd7f8" } },
    { types: ["number", "boolean", "constant", "symbol"], style: { color: "#ffb183" } },
    { types: ["property", "attr-name", "variable"], style: { color: "rgba(255, 255, 255, 0.97)" } },
    { types: ["inserted"], style: { color: "#b6d6b0" } },
    { types: ["deleted"], style: { color: "#ffa5a5" } },
  ],
};

const config = {
  title: "zodbase",
  tagline: "Typed tables and queries, powered by Zod.",
  favicon: "img/favicon.svg",
  url: "https://zodbase.knownquantity.net",
  baseUrl,
  organizationName: "Censkh",
  projectName: "zodbase",
  onBrokenLinks: "throw",
  trailingSlash: true,

  presets: [
    [
      "classic",
      {
        docs: {
          path: path.resolve(__dirname, "docs"),
          routeBasePath: "/",
          sidebarPath: require.resolve("./sidebars.js"),
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
          editUrl: "https://github.com/Censkh/zodbase/edit/master/website/docs/",
        },
        blog: false,
        pages: false,
        sitemap: { ignorePatterns: ["/search/**"] },
        theme: {
          customCss: require.resolve("./src/css/custom.css"),
        },
      },
    ],
  ],

  markdown: {
    hooks: {
      onBrokenMarkdownLinks: "throw",
    },
  },

  plugins: [
    [
      "docusaurus-plugin-copy-page-button",
      {
        injectButton: false,
        enabledActions: ["copy", "view"],
        generateMarkdownRoutes: true,
      },
    ],
    [
      "@easyops-cn/docusaurus-search-local",
      {
        hashed: true,
        indexDocs: true,
        docsDir: "docs",
        docsRouteBasePath: "/",
        indexBlog: false,
        highlightSearchTermsOnTargetPage: true,
        language: ["en"],
      },
    ],
  ],

  themeConfig: {
    image: "img/zodbase-social-card.png",
    metadata: [
      { property: "og:type", content: "website" },
      { property: "og:site_name", content: "zodbase" },
      { property: "og:locale", content: "en_GB" },
      { property: "og:image:type", content: "image/png" },
      { property: "og:image:width", content: "1200" },
      { property: "og:image:height", content: "630" },
      { property: "og:image:alt", content: "Zodbase — Your schema. Your database. Lavender and ice-blue schema cards on a dark background." },
      { name: "twitter:image:alt", content: "Zodbase — Your schema. Your database. Lavender and ice-blue schema cards on a dark background." },
      { name: "theme-color", content: "#12131f" },
    ],
    colorMode: { defaultMode: "dark", disableSwitch: true, respectPrefersColorScheme: false },
    navbar: {
      title: "zodbase",
      logo: { alt: "", src: "img/logo-mark.svg", width: 36, height: 36 },
      items: [
        {
          type: "docSidebar",
          sidebarId: "docs",
          position: "left",
          label: "Documentation",
        },
        {
          href: guideUrl,
          label: "Query guide",
          position: "left",
        },
        {
          href: "https://github.com/Censkh/zodbase",
          label: "GitHub",
          position: "right",
        },
      ],
    },
    prism: {
      theme: codeTheme,
      darkTheme: codeTheme,
      additionalLanguages: ["bash", "diff", "json"],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Resources",
          items: [
            { label: "Documentation", to: "/" },
            { label: "Query guide", href: guideUrl },
            { label: "GitHub", href: "https://github.com/Censkh/zodbase" },
          ],
        },
      ],
      copyright: `<div class="developer-credit"><div>Developed by <a href="https://github.com/Censkh">James Waterhouse</a> of <a href="https://knownquantity.net/">Known Quantity</a><br/><span>Copyright © ${new Date().getFullYear()} zodbase contributors.</span></div><a class="known-quantity" href="https://knownquantity.net/" aria-label="Known Quantity website"><img src="/img/known-quantity.svg" alt="Known Quantity" width="181" height="48" /></a></div>`,
    },
  },
};

module.exports = config;
