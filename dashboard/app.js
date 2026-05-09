const metricLabels = {
  articles_by_source: "Articles by Source",
  articles_by_topic: "Articles by Topic",
  articles_by_sentiment: "Articles by Sentiment",
  articles_by_language: "Articles by Language",
  articles_by_source_country: "Articles by Source Country",
  top_tags: "Top Tags",
  top_companies: "Top Companies",
  top_currencies: "Top Currencies",
  top_countries_mentioned: "Top Countries Mentioned",
};

const colors = {
  positive: "#00a88f",
  neutral: "#6d5dfc",
  negative: "#df4d4d",
  other: "#246bfe",
};

let state = null;

function number(value) {
  return new Intl.NumberFormat().format(value || 0);
}

function pct(value) {
  return `${Math.round((value || 0) * 100)}%`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function renderKpis(gold, quality) {
  const sources = gold.articles_by_source?.length || 0;
  const topTopic = gold.articles_by_topic?.[0]?.value || "n/a";
  const failed = quality.summary.failed_errors + quality.summary.failed_warnings;
  document.getElementById("kpis").innerHTML = `
    <article class="kpi">
      <div class="label">Articles</div>
      <div class="value">${number(gold.record_count)}</div>
      <div class="note">Gold snapshot rows</div>
    </article>
    <article class="kpi">
      <div class="label">Average Quality</div>
      <div class="value">${pct(gold.average_quality_score)}</div>
      <div class="note">Normalized score</div>
    </article>
    <article class="kpi">
      <div class="label">Sources</div>
      <div class="value">${number(sources)}</div>
      <div class="note">Top topic: ${escapeHtml(topTopic)}</div>
    </article>
    <article class="kpi">
      <div class="label">Quality Checks</div>
      <div class="value">${number(quality.summary.passed_checks)}/${number(quality.summary.total_checks)}</div>
      <div class="note">${number(failed)} failed or warning checks</div>
    </article>
  `;
}

function renderMetricSelector(gold) {
  const select = document.getElementById("metricSelect");
  select.innerHTML = Object.keys(metricLabels)
    .filter((key) => Array.isArray(gold[key]))
    .map((key) => `<option value="${key}">${metricLabels[key]}</option>`)
    .join("");
  select.value = "articles_by_source";
  select.addEventListener("change", () => renderBars(select.value));
}

function renderBars(metric) {
  const rows = state.gold[metric] || [];
  const max = Math.max(...rows.map((row) => row.count), 1);
  const total = rows.reduce((acc, row) => acc + (row.count || 0), 0);
  document.getElementById("metricTitle").textContent = metricLabels[metric] || metric;
  document.getElementById("metricTotal").textContent = `${number(total)} counted`;
  document.getElementById("barChart").innerHTML = rows
    .map((row) => {
      const width = Math.max(2, ((row.count || 0) / max) * 100);
      return `
        <div class="bar-row" title="${escapeHtml(row.value)}">
          <div class="bar-label">${escapeHtml(row.value)}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
          <div class="bar-value">${number(row.count)}</div>
        </div>
      `;
    })
    .join("");
}

function renderDonut(gold) {
  const rows = gold.articles_by_sentiment || [];
  const total = rows.reduce((acc, row) => acc + row.count, 0) || 1;
  let cursor = 0;
  const stops = rows
    .map((row) => {
      const start = cursor;
      cursor += (row.count / total) * 100;
      const color = colors[row.value] || colors.other;
      return `${color} ${start}% ${cursor}%`;
    })
    .join(", ");
  document.getElementById("donutChart").innerHTML = `
    <div class="donut" style="background: conic-gradient(${stops});"></div>
    <div class="legend">
      ${rows
        .map(
          (row) => `
          <div class="legend-row">
            <span class="legend-left"><span class="dot" style="background:${colors[row.value] || colors.other}"></span>${escapeHtml(row.value)}</span>
            <strong>${number(row.count)}</strong>
          </div>
        `,
        )
        .join("")}
    </div>
  `;
}

function renderSourceSentiment(gold) {
  const rows = gold.average_sentiment_by_source || [];
  document.getElementById("sourceSentiment").innerHTML = rows
    .map((row) => {
      const score = Number(row.average_sentiment_score || 0);
      return `
        <div class="score-row">
          <span><strong>${escapeHtml(row.source)}</strong><br>${number(row.article_count)} articles</span>
          <span>${score.toFixed(3)}</span>
        </div>
      `;
    })
    .join("");
}

function renderArticles() {
  const search = document.getElementById("articleSearch").value.trim().toLowerCase();
  const rows = (state.gold.latest_articles || []).filter((article) => {
    const haystack = `${article.source} ${article.topic} ${article.sentiment} ${article.title}`.toLowerCase();
    return haystack.includes(search);
  });
  document.getElementById("articleCount").textContent = `${number(rows.length)} shown`;
  document.getElementById("articlesTable").innerHTML = rows
    .map(
      (article) => `
      <tr>
        <td>${escapeHtml(article.source)}</td>
        <td>${escapeHtml(article.topic)}</td>
        <td><span class="pill ${escapeHtml(article.sentiment)}">${escapeHtml(article.sentiment)}</span></td>
        <td>${pct(article.quality_score)}</td>
        <td><a href="${escapeHtml(article.url)}" target="_blank" rel="noreferrer">${escapeHtml(article.title)}</a></td>
      </tr>
    `,
    )
    .join("");
}

function renderQuality(quality) {
  const summary = quality.summary;
  const badgeClass = summary.status === "passed" ? "passed" : summary.status === "warning" ? "warning" : "failed";
  document.getElementById("qualityBadge").innerHTML = `<span class="pill ${badgeClass}">${summary.status}</span>`;
  document.getElementById("qualitySummary").innerHTML = `
    <div class="quality-row"><span>Passed</span><strong>${number(summary.passed_checks)}</strong></div>
    <div class="quality-row"><span>Error failures</span><strong>${number(summary.failed_errors)}</strong></div>
    <div class="quality-row"><span>Warning failures</span><strong>${number(summary.failed_warnings)}</strong></div>
  `;

  const failed = quality.checks.filter((check) => check.status === "failed");
  document.getElementById("qualityChecks").innerHTML = (failed.length ? failed : quality.checks.slice(0, 6))
    .map(
      (check) => `
      <div class="quality-row">
        <span>
          <strong>${escapeHtml(check.check_name)}</strong>
          <small>${escapeHtml(check.dimension)}: ${escapeHtml(check.details)}</small>
        </span>
        <span class="pill ${check.status === "passed" ? "passed" : check.severity === "warning" ? "warning" : "failed"}">${escapeHtml(check.status)}</span>
      </div>
    `,
    )
    .join("");
}

async function init() {
  const response = await fetch("data.json", { cache: "no-store" });
  state = await response.json();
  const { gold, quality } = state;
  document.getElementById("runMeta").innerHTML = `
    Generated: <strong>${escapeHtml(gold.generated_at)}</strong><br>
    Engine: ${escapeHtml(gold.engine)}
  `;

  renderKpis(gold, quality);
  renderMetricSelector(gold);
  renderBars("articles_by_source");
  renderDonut(gold);
  renderSourceSentiment(gold);
  renderArticles();
  renderQuality(quality);

  document.getElementById("articleSearch").addEventListener("input", renderArticles);
}

init().catch((error) => {
  document.body.innerHTML = `<main><div class="panel"><h1>Dashboard data unavailable</h1><p>${escapeHtml(error.message)}</p></div></main>`;
});
