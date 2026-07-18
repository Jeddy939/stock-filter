const concepts = {
  workbench: {
    name: "Research Workbench",
    description: "A professional market workstation for scanning, chart inspection and rapid appraisal."
  },
  ledger: {
    name: "Investment Ledger",
    description: "A quieter, light research environment with report-like hierarchy and highly legible tables."
  },
  review: {
    name: "Signal Review",
    description: "A focused appraisal workflow that adds a decision rail for working through every qualified stock."
  }
};

const marketRows = {
  asx: [
    {
      ticker: "CBA", exchange: "ASX", mark: "AU", name: "Commonwealth Bank of Australia", sector: "Financial Services · Banks",
      price: 168.42, change: 1.18, volume: 2.86, cap: "$300.4B", history: "Full · 700w", rating: "winner",
      summary: "Australia's largest retail bank, providing consumer, business and institutional banking services across Australia and New Zealand."
    },
    {
      ticker: "OIL", exchange: "ASX", mark: "AU", name: "Optiscan Imaging", sector: "Healthcare · Medical Devices",
      price: 0.16, change: 6.67, volume: 4.18, cap: "$135.7M", history: "Full · 700w", rating: "potential_winner",
      summary: "Develops microscopic imaging technology designed to support real-time tissue assessment during clinical procedures."
    },
    {
      ticker: "CGS", exchange: "ASX", mark: "AU", name: "Cogstate", sector: "Healthcare · Diagnostics",
      price: 2.90, change: 3.21, volume: 3.12, cap: "$462.1M", history: "Full · 700w", rating: "needs_confirmation",
      summary: "Provides digital cognitive assessment tools used in clinical trials, academic research and healthcare settings."
    },
    {
      ticker: "DRO", exchange: "ASX", mark: "AU", name: "DroneShield", sector: "Industrials · Aerospace & Defence",
      price: 3.84, change: 8.47, volume: 5.62, cap: "$3.6B", history: "Full · 360w", rating: "winner",
      summary: "Designs counter-drone detection and defeat systems for defence, government and critical-infrastructure customers."
    },
    {
      ticker: "RUL", exchange: "ASX", mark: "AU", name: "RPMGlobal Holdings", sector: "Technology · Software",
      price: 3.18, change: 2.25, volume: 2.43, cap: "$785.0M", history: "Full · 700w", rating: "maybe",
      summary: "Develops mine planning, asset management and financial modelling software for global mining operations."
    },
    {
      ticker: "WTC", exchange: "ASX", mark: "AU", name: "WiseTech Global", sector: "Technology · Logistics Software",
      price: 83.71, change: -1.04, volume: 2.17, cap: "$28.0B", history: "Full · 700w", rating: "bad",
      summary: "Provides cloud-based logistics execution software used by freight forwarders and supply-chain companies worldwide."
    }
  ],
  us: [
    {
      ticker: "IBRX", exchange: "NASDAQ", mark: "US", name: "ImmunityBio", sector: "Healthcare · Biotechnology",
      price: 4.86, change: 4.07, volume: 3.91, cap: "$5.1B", history: "Young · 180w", rating: "winner",
      summary: "Develops immune-based therapies intended to activate the innate and adaptive immune systems against cancer and infectious disease."
    },
    {
      ticker: "AAPL", exchange: "NASDAQ", mark: "US", name: "Apple", sector: "Technology · Consumer Electronics",
      price: 211.18, change: 0.82, volume: 2.32, cap: "$3.2T", history: "Full · 700w", rating: "potential_winner",
      summary: "Designs consumer electronics, operating systems and digital services, including iPhone, Mac, wearables and the App Store."
    },
    {
      ticker: "PLTR", exchange: "NASDAQ", mark: "US", name: "Palantir Technologies", sector: "Technology · Infrastructure Software",
      price: 147.21, change: 2.74, volume: 2.68, cap: "$351.0B", history: "Young · 360w", rating: "needs_confirmation",
      summary: "Builds data integration and decision platforms used by governments and commercial organisations."
    },
    {
      ticker: "RKLB", exchange: "NASDAQ", mark: "US", name: "Rocket Lab", sector: "Industrials · Aerospace",
      price: 43.65, change: 5.46, volume: 4.22, cap: "$22.8B", history: "Young · 180w", rating: "winner",
      summary: "Provides launch services, spacecraft components and end-to-end space systems for commercial and government customers."
    },
    {
      ticker: "SOFI", exchange: "NASDAQ", mark: "US", name: "SoFi Technologies", sector: "Financial Services · Fintech",
      price: 19.84, change: 1.38, volume: 2.54, cap: "$22.0B", history: "Young · 360w", rating: "maybe",
      summary: "Operates a digital financial-services platform spanning lending, banking, investing and financial technology infrastructure."
    },
    {
      ticker: "RIVN", exchange: "NASDAQ", mark: "US", name: "Rivian Automotive", sector: "Consumer Cyclical · Auto Manufacturers",
      price: 13.07, change: -2.17, volume: 2.09, cap: "$16.4B", history: "Young · 180w", rating: "bad",
      summary: "Designs and manufactures electric adventure vehicles, delivery vans and related software and charging services."
    }
  ]
};

const ratingDefinitions = [
  ["winner", "W", "Winner"],
  ["potential_winner", "P", "Potential winner"],
  ["needs_confirmation", "C", "Needs confirmation"],
  ["maybe", "M", "Maybe"],
  ["bad", "B", "Bad"]
];

let currentConcept = "workbench";
let currentMarket = "asx";
let activeIndex = 0;
let operationTimer = null;

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

function refreshIcons() {
  if (window.lucide) window.lucide.createIcons({attrs: {"stroke-width": 1.7}});
}

function setConcept(concept, updateUrl = true) {
  if (!concepts[concept]) return;
  currentConcept = concept;
  document.body.dataset.concept = concept;
  $$("[data-concept-choice]").forEach((button) => {
    const active = button.dataset.conceptChoice === concept;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", String(active));
  });
  $("#conceptDescription").textContent = concepts[concept].description;
  if (updateUrl) {
    const url = new URL(window.location.href);
    url.searchParams.set("concept", concept);
    history.replaceState({}, "", url);
  }
  window.requestAnimationFrame(drawChart);
}

function selectedConceptName() {
  const selected = localStorage.getItem("moneymaker-design-choice");
  return concepts[selected]?.name || "None";
}

function updateSelectedConcept() {
  $("#selectedConcept").textContent = selectedConceptName();
}

function setView(view) {
  $$("[data-view]").forEach((button) => {
    const active = button.dataset.view === view;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", String(active));
  });
  $$("[data-app-view]").forEach((section) => {
    const active = section.dataset.appView === view;
    section.hidden = !active;
    section.classList.toggle("active", active);
  });
  if (view === "market") window.requestAnimationFrame(drawChart);
}

function currentRows() {
  return marketRows[currentMarket];
}

function yahooTicker(row) {
  return row.exchange === "ASX" ? `${row.ticker}.AX` : row.ticker;
}

function selectStock(index) {
  const rows = currentRows();
  activeIndex = (index + rows.length) % rows.length;
  const row = rows[activeIndex];
  $("#exchangeMark").textContent = row.mark;
  $("#activeTicker").textContent = row.ticker;
  $("#activeExchange").textContent = row.exchange;
  $("#activePrice").textContent = `$${row.price.toFixed(2)}`;
  $("#activeChange").textContent = `${row.change >= 0 ? "+" : ""}${row.change.toFixed(2)}%`;
  $("#activeChange").className = row.change >= 0 ? "positive" : "negative";
  $("#activeCompany").textContent = `${row.name} · ${row.sector}`;
  $("#companySummary").textContent = row.summary;
  $("#reviewTicker").textContent = row.ticker;
  $("#queueIndex").textContent = String(activeIndex + 1);
  $("#reviewNote").value = row.note || "";
  $("#yahooLink").href = `https://finance.yahoo.com/quote/${encodeURIComponent(yahooTicker(row))}`;
  $$("#reviewActions [data-rating]").forEach((button) => button.classList.toggle("active", button.dataset.rating === row.rating));
  $$("#resultsBody tr").forEach((tableRow, rowIndex) => tableRow.classList.toggle("selected", rowIndex === activeIndex));
  window.requestAnimationFrame(drawChart);
}

function ratingButtons(row, index) {
  return `<div class="rating-control">${ratingDefinitions.map(([value, abbreviation, label]) => (
    `<button type="button" data-row-index="${index}" data-rating="${value}" class="${row.rating === value ? "active" : ""}" title="Mark as ${label}" aria-label="Mark ${row.ticker} as ${label}">${abbreviation}</button>`
  )).join("")}</div>`;
}

function renderResults(filter = "") {
  const normalized = filter.trim().toLowerCase();
  const rows = currentRows();
  const visibleRows = rows.map((row, index) => ({row, index})).filter(({row}) => (
    !normalized || row.ticker.toLowerCase().includes(normalized) || row.name.toLowerCase().includes(normalized)
  ));
  $("#resultCount").textContent = String(visibleRows.length);
  $("#resultsBody").innerHTML = visibleRows.map(({row, index}) => `
    <tr class="${index === activeIndex ? "selected" : ""}" data-result-index="${index}">
      <td>
        <div class="company-cell">
          <span class="exchange-mark">${row.mark}</span>
          <div><button type="button" data-select-stock="${index}">${row.ticker}</button><small>${row.name}</small></div>
        </div>
      </td>
      <td>$${row.price.toFixed(2)}</td>
      <td class="${row.change >= 0 ? "positive" : "negative"}">${row.change >= 0 ? "+" : ""}${row.change.toFixed(2)}%</td>
      <td>${row.volume.toFixed(2)}×</td>
      <td>${row.cap}</td>
      <td>${row.history}</td>
      <td>${ratingButtons(row, index)}</td>
      <td><button class="note-button" type="button" title="Edit private note" aria-label="Edit note for ${row.ticker}" data-note-index="${index}"><i data-lucide="notebook-pen"></i></button></td>
    </tr>
  `).join("") || `<tr><td colspan="8">No stocks match that search.</td></tr>`;

  $$('[data-select-stock]').forEach((button) => button.addEventListener("click", () => selectStock(Number(button.dataset.selectStock))));
  $$("#resultsBody [data-rating]").forEach((button) => button.addEventListener("click", () => {
    setRating(Number(button.dataset.rowIndex), button.dataset.rating);
  }));
  $$("#resultsBody [data-note-index]").forEach((button) => button.addEventListener("click", () => {
    const index = Number(button.dataset.noteIndex);
    selectStock(index);
    if (currentConcept !== "review") setConcept("review");
    $("#reviewNote").focus();
  }));
  refreshIcons();
}

function setRating(index, rating) {
  const row = currentRows()[index];
  row.rating = row.rating === rating ? "" : rating;
  renderResults($("#tickerSearch").value);
  if (index === activeIndex) {
    $$("#reviewActions [data-rating]").forEach((button) => button.classList.toggle("active", button.dataset.rating === row.rating));
  }
}

function setMarket(market) {
  if (!marketRows[market]) return;
  currentMarket = market;
  activeIndex = 0;
  $$('[data-market]').forEach((button) => button.classList.toggle("active", button.dataset.market === market));
  $$('[data-market-status]').forEach((button) => button.classList.toggle("active", button.dataset.marketStatus === market));
  $("#tickerSearch").value = "";
  renderResults();
  selectStock(0);
}

function hashTicker(value) {
  let hash = 2166136261;
  for (const character of value) {
    hash ^= character.charCodeAt(0);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function seededRandom(seed) {
  let state = seed || 1;
  return () => {
    state = Math.imul(1664525, state) + 1013904223 >>> 0;
    return state / 4294967296;
  };
}

function generateCandles(row, count = 86) {
  const random = seededRandom(hashTicker(`${row.ticker}-${currentConcept}`));
  const candles = [];
  let close = row.price * .72;
  for (let index = 0; index < count; index += 1) {
    const drift = row.change >= 0 ? .0028 : .0004;
    const cycle = Math.sin(index / 7) * .011;
    const open = close * (1 + (random() - .5) * .022);
    close = Math.max(row.price * .32, open * (1 + drift + cycle + (random() - .48) * .041));
    const high = Math.max(open, close) * (1 + random() * .022);
    const low = Math.min(open, close) * (1 - random() * .022);
    candles.push({open, high, low, close, volume: .28 + random() * .62 + (index % 21 === 0 ? .72 : 0)});
  }
  const scale = row.price / candles[candles.length - 1].close;
  return candles.map((candle) => ({
    open: candle.open * scale,
    high: candle.high * scale,
    low: candle.low * scale,
    close: candle.close * scale,
    volume: candle.volume
  }));
}

function movingAverage(candles, period) {
  return candles.map((candle, index) => {
    if (index < period - 1) return null;
    const slice = candles.slice(index - period + 1, index + 1);
    return slice.reduce((sum, value) => sum + value.close, 0) / period;
  });
}

function drawChart() {
  const canvas = $("#conceptChart");
  if (!canvas || canvas.offsetWidth === 0 || canvas.offsetHeight === 0) return;
  const style = getComputedStyle(document.body);
  const dpr = Math.max(1, window.devicePixelRatio || 1);
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  canvas.width = Math.round(width * dpr);
  canvas.height = Math.round(height * dpr);
  const context = canvas.getContext("2d");
  context.setTransform(dpr, 0, 0, dpr, 0, 0);
  context.clearRect(0, 0, width, height);
  context.fillStyle = style.getPropertyValue("--chart-bg").trim();
  context.fillRect(0, 0, width, height);

  const row = currentRows()[activeIndex];
  const candles = generateCandles(row);
  const chart = {left: 52, right: width - 14, top: 17, bottom: height - 83};
  const volume = {top: height - 65, bottom: height - 18};
  const values = candles.flatMap((candle) => [candle.high, candle.low]);
  const low = Math.min(...values) * .97;
  const high = Math.max(...values) * 1.03;
  const priceY = (value) => chart.bottom - ((value - low) / (high - low)) * (chart.bottom - chart.top);
  const xStep = (chart.right - chart.left) / candles.length;
  const gridColor = style.getPropertyValue("--chart-grid").trim();
  const muted = style.getPropertyValue("--muted").trim();

  context.strokeStyle = gridColor;
  context.fillStyle = muted;
  context.font = `10px ${style.getPropertyValue("--font-data")}`;
  context.lineWidth = 1;
  for (let index = 0; index <= 4; index += 1) {
    const y = chart.top + (chart.bottom - chart.top) * index / 4;
    const label = high - (high - low) * index / 4;
    context.beginPath();
    context.moveTo(chart.left, Math.round(y) + .5);
    context.lineTo(chart.right, Math.round(y) + .5);
    context.stroke();
    context.fillText(label.toFixed(label < 10 ? 2 : 1), 6, y + 3);
  }
  context.beginPath();
  context.moveTo(chart.left, volume.top - 8);
  context.lineTo(chart.right, volume.top - 8);
  context.stroke();

  candles.forEach((candle, index) => {
    const x = chart.left + xStep * (index + .5);
    const rising = candle.close >= candle.open;
    const color = rising ? style.getPropertyValue("--positive").trim() : style.getPropertyValue("--negative").trim();
    const bodyTop = priceY(Math.max(candle.open, candle.close));
    const bodyBottom = priceY(Math.min(candle.open, candle.close));
    context.strokeStyle = color;
    context.fillStyle = color;
    context.beginPath();
    context.moveTo(x, priceY(candle.high));
    context.lineTo(x, priceY(candle.low));
    context.stroke();
    context.fillRect(Math.round(x - Math.max(1.2, xStep * .28)), bodyTop, Math.max(2, xStep * .56), Math.max(1, bodyBottom - bodyTop));
    context.globalAlpha = .34;
    context.fillRect(Math.round(x - Math.max(1, xStep * .25)), volume.bottom - candle.volume * (volume.bottom - volume.top) / 1.2, Math.max(2, xStep * .5), candle.volume * (volume.bottom - volume.top) / 1.2);
    context.globalAlpha = 1;
  });

  const activeAverages = $$("[data-ma].active").map((button) => Number(button.dataset.ma));
  const periodMap = {30: 10, 90: 21, 180: 37, 360: 55, 700: 72};
  activeAverages.forEach((label) => {
    const button = $(`[data-ma="${label}"]`);
    const color = $("span", button).style.getPropertyValue("--ma");
    const valuesForLine = movingAverage(candles, periodMap[label]);
    context.strokeStyle = color;
    context.lineWidth = 1.6;
    context.beginPath();
    let started = false;
    valuesForLine.forEach((value, index) => {
      if (value === null) return;
      const x = chart.left + xStep * (index + .5);
      const y = priceY(value);
      if (!started) { context.moveTo(x, y); started = true; }
      else context.lineTo(x, y);
    });
    context.stroke();
  });

  context.fillStyle = muted;
  context.fillText("Volume", chart.left, volume.top - 15);
  context.textAlign = "right";
  context.fillText("17 JUL 2026", chart.right, height - 5);
  context.textAlign = "left";
}

function openOperations() {
  $("#operationsDrawer").classList.add("open");
  $("#operationsDrawer").setAttribute("aria-hidden", "false");
  $("#drawerScrim").classList.add("visible");
}

function closeOperations() {
  $("#operationsDrawer").classList.remove("open");
  $("#operationsDrawer").setAttribute("aria-hidden", "true");
  $("#drawerScrim").classList.remove("visible");
}

function updateOperation(stage) {
  const labels = [
    ["Queued", "Waiting for an available screening worker."],
    ["Loading market snapshot", "Reading the latest weekly metrics."],
    ["Applying screen criteria", "Evaluating 5,493 supported ticker rows."],
    ["Ranking matches", "Ordering 38 qualified stocks."],
    ["Complete", "The fresh screen is ready to inspect."]
  ];
  $("#operationTitle").textContent = labels[stage][0];
  $("#operationMessage").textContent = labels[stage][1];
  $("#operationFill").style.width = `${stage * 25}%`;
  $$("#operationStages li").forEach((item, index) => {
    item.classList.toggle("complete", index < stage || stage === 4);
    item.classList.toggle("active", index === stage && stage < 4);
  });
  $("#operationsDrawer").classList.toggle("running", stage < 4);
}

function runScreenDemo() {
  if (operationTimer) window.clearInterval(operationTimer);
  let stage = 0;
  updateOperation(stage);
  openOperations();
  operationTimer = window.setInterval(() => {
    stage += 1;
    updateOperation(stage);
    if (stage >= 4) {
      window.clearInterval(operationTimer);
      operationTimer = null;
    }
  }, 900);
}

function bindControls() {
  $$("[data-concept-choice]").forEach((button) => button.addEventListener("click", () => setConcept(button.dataset.conceptChoice)));
  $("#selectDirection").addEventListener("click", () => {
    localStorage.setItem("moneymaker-design-choice", currentConcept);
    updateSelectedConcept();
    $("#selectedConcept").scrollIntoView({block: "nearest", behavior: "smooth"});
  });
  $("#clearChoice").addEventListener("click", () => {
    localStorage.removeItem("moneymaker-design-choice");
    updateSelectedConcept();
  });
  $$("[data-view]").forEach((button) => button.addEventListener("click", () => setView(button.dataset.view)));
  $$("[data-market]").forEach((button) => button.addEventListener("click", () => setMarket(button.dataset.market)));
  $$("[data-market-status]").forEach((button) => button.addEventListener("click", () => setMarket(button.dataset.marketStatus)));
  $("#tickerSearch").addEventListener("input", (event) => renderResults(event.target.value));
  $("#toggleSettings").addEventListener("click", () => { $("#filterTray").hidden = !$("#filterTray").hidden; });
  $$("[data-interval], [data-range]").forEach((button) => button.addEventListener("click", () => {
    const selector = button.hasAttribute("data-interval") ? "[data-interval]" : "[data-range]";
    $$(selector).forEach((control) => control.classList.toggle("active", control === button));
    drawChart();
  }));
  $$("[data-ma]").forEach((button) => button.addEventListener("click", () => { button.classList.toggle("active"); drawChart(); }));
  $$("#reviewActions [data-rating]").forEach((button) => button.addEventListener("click", () => setRating(activeIndex, button.dataset.rating)));
  $("#reviewNote").addEventListener("input", (event) => { currentRows()[activeIndex].note = event.target.value; });
  $("#previousStock").addEventListener("click", () => selectStock(activeIndex - 1));
  $("#saveAndNext").addEventListener("click", () => selectStock(activeIndex + 1));
  $("#openOperations").addEventListener("click", openOperations);
  $("#closeOperations").addEventListener("click", closeOperations);
  $("#drawerScrim").addEventListener("click", closeOperations);
  $("#runFreshScreen").addEventListener("click", runScreenDemo);
  window.addEventListener("keydown", (event) => { if (event.key === "Escape") closeOperations(); });
  let resizeTimer = null;
  window.addEventListener("resize", () => {
    window.clearTimeout(resizeTimer);
    resizeTimer = window.setTimeout(drawChart, 80);
  });
}

function initialise() {
  const requestedConcept = new URL(window.location.href).searchParams.get("concept");
  setConcept(concepts[requestedConcept] ? requestedConcept : "workbench", false);
  updateSelectedConcept();
  bindControls();
  renderResults();
  selectStock(0);
  refreshIcons();
  window.requestAnimationFrame(drawChart);
}

initialise();
