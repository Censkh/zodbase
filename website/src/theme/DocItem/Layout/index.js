import { useDoc } from "@docusaurus/plugin-content-docs/client";
import IconArrowDown from "@theme/Icon/ArrowDown";
import DocItemLayout from "@theme-original/DocItem/Layout";
import React, { useEffect } from "react";

export default function DocItemLayoutWrapper(props) {
  const { metadata } = useDoc();
  useEffect(() => {
    const onKeyDown = (event) => {
      if (event.key !== "Escape") return;
      const trigger = document.querySelector('[data-copy-page-button-trigger][aria-expanded="true"]');
      if (trigger) {
        event.preventDefault();
        trigger.click();
        trigger.focus();
      }
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);
  return (
    <>
      {metadata.id === "getting-started" && (
        <section className="landing-hero zodbase-hero" aria-labelledby="hero-title">
          <div className="schema-heading">
            <img src="/img/logo-mark.svg" alt="" width="72" height="72" />
            <h1 id="hero-title">
              Your schema.
              <br />
              <span>Your database.</span>
            </h1>
          </div>
          <p className="landing-description">
            Zod schemas become typed tables. Build queries, evolve schemas, and work across database adaptors with one
            explicit model.
          </p>
          <div className="landing-actions">
            <a className="button button--primary" href="#install">
              Build your first table <IconArrowDown />
            </a>
            <a className="button button--secondary" href="/queries/">
              Explore queries
            </a>
          </div>
          <section className="schema-preview" aria-label="Users table example">
            <div className="schema-preview-title">
              <code>users</code>
              <a href="/tables-and-schemas/">Table definition</a>
            </div>
            <dl>
              <div>
                <dt>id</dt>
                <dd>
                  string <span>primary key</span>
                </dd>
              </div>
              <div>
                <dt>name</dt>
                <dd>string</dd>
              </div>
              <div>
                <dt>active</dt>
                <dd>
                  boolean <span>default true</span>
                </dd>
              </div>
            </dl>
            <a className="schema-query" href="/queries/">
              <code>db.select(Users)</code>
            </a>
          </section>
        </section>
      )}
      <DocItemLayout {...props} />
    </>
  );
}
