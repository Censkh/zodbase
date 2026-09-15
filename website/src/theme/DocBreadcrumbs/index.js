import React from "react";
import DocBreadcrumbs from "@theme-original/DocBreadcrumbs";
import CopyPageButton from "docusaurus-plugin-copy-page-button/react";

export default function DocToolbar() {
  return (
    <div className="doc-toolbar">
      <DocBreadcrumbs />
      <CopyPageButton enabledActions={["copy", "view"]} generateMarkdownRoutes />
    </div>
  );
}
