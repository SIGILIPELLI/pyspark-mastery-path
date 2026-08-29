---
title: "Learn PySpark Free: Beginner to Master Course"
description: "Free course on PySpark and Apache Spark for big-data processing -- DataFrames, joins, window functions, Structured Streaming, and performance tuning, with real hands-on code."
---

# PySpark Mastery Path

A structured, module-wise training program on **PySpark**, Apache Spark's Python
API for distributed, big-data processing — from your first `SparkSession` to
production-grade Spark jobs, streaming pipelines, and lakehouse patterns — with
real code in every module and a hands-on project at the end of each level.

Spark is the engine behind most large-scale data pipelines you'll meet in the
wild: it takes a dataset too big (or too slow) for a single machine and spreads
the work across a cluster, while giving you an API that still reads like
ordinary DataFrame code. This site teaches Spark from first principles — what a
driver and an executor actually do, why a shuffle is expensive, why joins pick
different physical strategies — not just which method to call.

## How the program is organized

| Level | Focus | Modules |
|-------|-------|---------|
| [Level 1 · Entry](level-1/index.md) | Spark architecture, SparkSession, RDDs vs. DataFrames, reading/writing data, DataFrame basics, schemas, aggregations | 9 topics + 1 capstone |
| [Level 2 · Intermediate](level-2/index.md) | Joins (broadcast vs. shuffle), window functions, UDFs, partitioning strategy, caching & persistence, Spark SQL | 9 topics + 1 capstone |
| [Level 3 · Advanced](level-3/index.md) | Execution plans (`explain()`), shuffle optimization, data skew, Structured Streaming, performance tuning | 9 topics + 1 capstone |
| [Level 4 · Master](level-4/index.md) | Delta Lake / lakehouse patterns, orchestration with Airflow, cost/performance tradeoffs at scale, debugging failed jobs | 9 topics + 1 capstone |

## What you need

- **Python 3.10+**, `pip`, and a Java runtime (JDK 11 or 17) — Spark runs on the
  JVM even when you drive it from Python.
- `pip install pyspark` gives you a local, single-machine Spark you can run on
  a laptop — no cluster, no cloud account needed for Level 1.
- Later levels introduce Delta Lake, Structured Streaming, and orchestration
  concepts; each lesson states exactly what to install before you start.

## How to use this site

- Work through each level in order — later modules assume earlier ones.
- Every topic page has real, syntactically-checked PySpark code. Code that was
  reasoned through carefully but not executed against a live cluster in this
  environment is labeled as such, so you always know what you're looking at.
- Each level ends with a project that combines everything learned in that level.
- Use the search bar (top of the page) to jump straight to a topic.

Start here → [Level 1 · Entry](level-1/index.md)

## Related tracks

PySpark sits next to data engineering, SQL, and distributed systems more
broadly. Sister sites cover the neighboring ground:

- [Data Engineering Mastery Path](https://sigilipelli.github.io/data-engineering-mastery-path/) — ETL pipelines, orchestration, and data platforms
- [SQL Mastery Path](https://sigilipelli.github.io/sql-mastery-path/) — SQL from first principles
- [AI/ML Mastery Path](https://sigilipelli.github.io/ai-ml-mastery-path/) — machine-learning foundations
- [Python Mastery Path](https://sigilipelli.github.io/python-mastery-path/) — Python from first principles

🎥 **Prefer video?** Watch the [Mastery Path video series](https://youtube.com/@sigilipelli) on YouTube — Shorts and full walkthroughs of these lessons.

## More from the Mastery Path series

Free, structured, module-wise training across 59 other languages, platforms and disciplines:

<div class="mastery-grid-wrap">
<p class="mastery-grid-category">Languages</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/python-mastery-path/">🐍 Python</a>
  <a href="https://sigilipelli.github.io/java-mastery-path/">☕ Java</a>
  <a href="https://sigilipelli.github.io/javascript-mastery-path/">🟨 JavaScript</a>
  <a href="https://sigilipelli.github.io/typescript-mastery-path/">🔷 TypeScript</a>
  <a href="https://sigilipelli.github.io/csharp-mastery-path/">🔵 C#</a>
  <a href="https://sigilipelli.github.io/shell-mastery-path/">🐚 Shell/Bash</a>
  <a href="https://sigilipelli.github.io/powershell-mastery-path/">💻 PowerShell</a>
  <a href="https://sigilipelli.github.io/c-mastery-path/">🇨 C</a>
  <a href="https://sigilipelli.github.io/cpp-mastery-path/">➕ C++</a>
  <a href="https://sigilipelli.github.io/go-mastery-path/">🐹 Go</a>
  <a href="https://sigilipelli.github.io/rust-mastery-path/">🦀 Rust</a>
  <a href="https://sigilipelli.github.io/sql-mastery-path/">🗄️ SQL</a>
  <a href="https://sigilipelli.github.io/ruby-mastery-path/">💎 Ruby</a>
  <a href="https://sigilipelli.github.io/php-mastery-path/">🐘 PHP</a>
  <a href="https://sigilipelli.github.io/kotlin-mastery-path/">🟣 Kotlin</a>
  <a href="https://sigilipelli.github.io/swift-mastery-path/">🐦 Swift</a>
  <a href="https://sigilipelli.github.io/dart-mastery-path/">🎯 Dart</a>
  <a href="https://sigilipelli.github.io/scala-mastery-path/">🔴 Scala</a>
  <a href="https://sigilipelli.github.io/r-mastery-path/">📊 R</a>
  <a href="https://sigilipelli.github.io/matlab-mastery-path/">🟧 MATLAB</a>
</div>
<p class="mastery-grid-category">Testing & QA</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/java-testing-mastery-path/">🧪 Java Testing</a>
  <a href="https://sigilipelli.github.io/cpp-testing-mastery-path/">🧪 C/C++ Testing</a>
  <a href="https://sigilipelli.github.io/python-testing-mastery-path/">🧪 Python Testing</a>
  <a href="https://sigilipelli.github.io/automotive-testing-mastery-path/">🚗 Automotive Testing</a>
</div>
<p class="mastery-grid-category">Security</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/cybersecurity-mastery-path/">🛡️ Cybersecurity</a>
</div>
<p class="mastery-grid-category">Cloud Platforms</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/aws-mastery-path/">☁️ AWS</a>
  <a href="https://sigilipelli.github.io/azure-mastery-path/">☁️ Azure</a>
  <a href="https://sigilipelli.github.io/gcp-mastery-path/">☁️ GCP</a>
  <a href="https://sigilipelli.github.io/ibm-cloud-mastery-path/">☁️ IBM Cloud</a>
  <a href="https://sigilipelli.github.io/adobe-mastery-path/">🎨 Adobe</a>
</div>
<p class="mastery-grid-category">Data & Analytics</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/data-engineering-mastery-path/">🛠️ Data Engineering</a>
  <a href="https://sigilipelli.github.io/data-science-mastery-path/">📈 Data Science</a>
  <a href="https://sigilipelli.github.io/tableau-mastery-path/">📊 Tableau</a>
  <a href="https://sigilipelli.github.io/excel-mastery-path/">📗 Excel</a>
  <a href="https://sigilipelli.github.io/etl-datalake-mastery-path/">🏞️ ETL & Data Lake</a>
</div>
<p class="mastery-grid-category">AI / ML / LLM</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/ai-ml-mastery-path/">🤖 AI/ML</a>
  <a href="https://sigilipelli.github.io/llm-dev-mastery-path/">🧠 LLM Dev</a>
  <a href="https://sigilipelli.github.io/rag-mastery-path/">📚 RAG</a>
  <a href="https://sigilipelli.github.io/edge-ai-mastery-path/">📱 Edge AI</a>
  <a href="https://sigilipelli.github.io/claude-training-mastery-path/">🔶 Claude Training</a>
  <a href="https://sigilipelli.github.io/ai-tools-mastery-path/">🧰 AI Tools</a>
  <a href="https://sigilipelli.github.io/ml-math-mastery-path/">➗ ML Math Foundations</a>
</div>
<p class="mastery-grid-category">Embedded Systems</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/embedded-mastery-path/">🔌 Embedded</a>
  <a href="https://sigilipelli.github.io/embedded-linux-mastery-path/">🐧 Embedded Linux</a>
  <a href="https://sigilipelli.github.io/embedded-python-mastery-path/">🐍 Embedded Python</a>
  <a href="https://sigilipelli.github.io/freertos-mastery-path/">⏱️ FreeRTOS</a>
  <a href="https://sigilipelli.github.io/s32k-mastery-path/">🔧 S32K</a>
</div>
<p class="mastery-grid-category">Leadership & Management</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/product-manager-mastery-path/">📋 Product Manager</a>
  <a href="https://sigilipelli.github.io/product-lead-mastery-path/">🧭 Product Lead</a>
  <a href="https://sigilipelli.github.io/project-manager-mastery-path/">📅 Project Manager</a>
  <a href="https://sigilipelli.github.io/ai-manager-mastery-path/">🤖 AI Manager</a>
  <a href="https://sigilipelli.github.io/servant-leadership-mastery-path/">🤝 Servant Leadership</a>
</div>
<p class="mastery-grid-category">Professional Skills</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/english-fluency-mastery-path/">🗣️ English Fluency & IELTS</a>
  <a href="https://sigilipelli.github.io/workday-mastery-path/">🧑‍💼 Workday</a>
</div>
<p class="mastery-grid-category">Process & APIs</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/agile-mastery-path/">🔄 Agile/Scrum/Kanban</a>
  <a href="https://sigilipelli.github.io/rest-api-mastery-path/">🔗 REST API</a>
  <a href="https://sigilipelli.github.io/playwright-mastery-path/">🎭 Playwright</a>
</div>
<p class="mastery-grid-category">Infrastructure & Ops</p>
<div class="mastery-grid">
  <a href="https://sigilipelli.github.io/server-ops-mastery-path/">🖥️ Server Ops</a>
  <a href="https://sigilipelli.github.io/nodemcu-mastery-path/">📶 NodeMCU/IoT</a>
</div>
</div>
