# MoneyMaker Commercial UI Audit

Date: 18 July 2026

## Product Position

MoneyMaker is not a generic dashboard. Its distinct workflow is:

1. Open the latest precomputed ASX or US screen.
2. Inspect each qualified company and its chart.
3. Record a private appraisal and rationale.
4. Measure whether the appraisals were accurate over time.
5. Discover which signal characteristics are shared by successful picks.

The interface should feel like an investment research workstation built around this review loop. It should not look like a marketing site, a general admin template, or a collection of decorative cards.

## Current Strengths

- The chart and results table now form the primary workspace.
- ASX and US are explicit and easy to switch.
- Appraisals are attached directly to screened stocks.
- Exact data dates are available instead of vague freshness labels.
- Long processes have persistent status and technical details.
- Advanced criteria are separated from the default screening path.
- The Analysis area has the right raw ingredients: labels, outcomes and group comparison.

## Current UX Problems

### 1. Repeated panels flatten the hierarchy

Most sections use the same border, fill, radius and heading treatment. The chart, results, status, metrics and administrative tools therefore appear equally important. A commercial interface needs stronger hierarchy through page bands, table structure, spacing and typography, with framed panels used only where a boundary is genuinely useful.

### 2. Data freshness is duplicated

Market refresh cards and the four metric tiles repeat related information. The primary workspace only needs the market date and last successful refresh at a glance. Coverage, weekly-row counts, scheduler details and failures belong in a hover/focus disclosure and the Operations view.

### 3. The screen command lacks a clear object

The interface has market selection, search, latest results and fresh-screen controls, but it does not clearly name the active screen as a reusable object. A commercial version should support named screen presets, a criteria summary, last-run metadata, and an explicit distinction between opening saved results and calculating a fresh result.

### 4. Appraisal is visually secondary

Rating buttons sit at the far edge of a wide table. That works mechanically but understates the product's differentiator. Appraisal should become a deliberate review control with strong labels, private-note context, keyboard support, and a queue state such as `12 of 38 reviewed`.

### 5. The table is not yet a research instrument

The table needs sortable columns, sticky identity and appraisal columns, column preferences, compact/comfortable density, saved views, export, and clear younger-stock MA tiers. IBM Carbon's data-table guidance specifically recommends giving dense tables substantial page width and pairing them with search, filtering and settings in a table toolbar.

### 6. Analysis reports data but does not guide decisions

The most valuable commercial output is not an average across every label. It is evidence about whether `Winner` and `Potential winner` selections work. The Analysis view should prioritise:

- Winner hit rate and median return by horizon.
- Performance against a benchmark and against all screened stocks.
- Cohorts by selection month and market regime.
- Winner survival curves at 30, 90, 180 and 360 days.
- A review queue for winners that have broken the original signal.
- A "winner fingerprint" showing recurring volume, MA, cap, sector and age characteristics.
- Analyst calibration, where sample size is sufficient.
- Clear separation of observations from statistically credible findings.

### 7. Brand identity is currently only a name and dark palette

The product needs a restrained visual signature that remains recognisable without a large logo. Recommended ingredients are a compact `MM` market mark, a disciplined data typeface, one characteristic accent, exchange markers, direct financial language, and consistent chart/table behaviour. Avoid gradients, glowing effects, oversized rounded cards, decorative blobs and promotional copy inside the application.

## Design Directions

### Option 1: Research Workbench

Best for frequent screening and technical review.

- Dense dark workstation.
- Compact command bar and market status line.
- Full-width chart followed by a research table.
- Monospace reserved for tickers, prices and measurements.
- Minimal framing and no decorative dashboard cards.

This is the recommended default because it best matches the current workflow and the user's preference for a full-width chart and results table.

### Option 2: Investment Ledger

Best for less technical investors, reports and client-facing demonstrations.

- Light neutral interface with stronger document hierarchy.
- More space around company identity and descriptions.
- Zebra-assisted table scanning and quieter controls.
- Better compatibility with print, export and daytime use.

This direction is easier to present commercially but is less terminal-like for long screening sessions.

### Option 3: Signal Review

Best if structured appraisal becomes the core commercial differentiator.

- Dark neutral workspace with a dedicated decision rail.
- Review queue position, conviction choices and private note visible beside the chart.
- `Save and next` workflow for processing a complete scan consistently.
- Full results table remains available below the review workspace.

This direction adds the most product differentiation but changes the current workflow more substantially.

## Established Product Patterns Used

The prototypes are original, but they use proven interaction ideas from established products and open design systems:

- [TradingView Stock Screener](https://www.tradingview.com/support/solutions/43000718866-tradingview-stock-screener-trade-smarter-not-harder/): named screens, hideable filters, configurable columns and market selection.
- [TradingView Supercharts](https://www.tradingview.com/support/solutions/43000746464-getting-started-with-supercharts/): compact chart controls and access to screening without leaving chart context.
- [Finviz screen and chart integration](https://finviz.com/blog/access-stock-screens-from-advanced-charts-on-finviz/): quick movement through filtered symbols while keeping the chart in context.
- [Koyfin functionality](https://www.koyfin.com/help/topic/functionality/): reusable views, linked dashboard components and research workspaces.
- [IBM Carbon data tables](https://carbondesignsystem.com/components/data-table/usage/): table sizing, toolbars, sorting, expandable rows and keyboard accessibility.
- [Lucide](https://lucide.dev/): consistent open-source interface icons instead of hand-drawn symbols.

## Improvements Required Regardless of Direction

### Product workflow

- Add named and saved screen presets.
- Add `Reviewed / total` progress for every scan.
- Preserve table sorting, filters and selected ticker between sessions.
- Support keyboard navigation through results and appraisal actions.
- Add a clear empty state for a valid screen with zero matches.
- Distinguish private notes from shared screen data everywhere.

### Analysis

- Store benchmark prices for every appraisal horizon.
- Track the original signal configuration with each appraisal.
- Record label changes as an immutable timeline.
- Show sample size and confidence warnings on every inferred pattern.
- Separate active picks, completed outcomes and superseded ratings.
- Add a winner-focused review queue and reclassification history.

### Commercial readiness

- Confirm commercial market-data licensing before charging users.
- Add terms, privacy policy, risk disclaimer and data-delay disclosure.
- Define subscription entitlements and enforce them server-side.
- Add account invitation, password reset, deletion and export workflows.
- Add support contact, incident messaging and service-status visibility.
- Add audit logs for administrative operations and market refreshes.
- Establish accessibility targets and test keyboard/screen-reader paths.
- Add product analytics for activation, screens viewed, reviews completed and retained usage without recording private note content.

## Questions To Resolve Before Production Redesign

1. Is the primary paying customer an individual investor, a small research group, or an adviser managing client research?
2. Should MoneyMaker feel more like a technical market terminal or a calm investment-research notebook?
3. Is the core promise finding unusual-volume technical setups, improving the accuracy of human stock selection, or both?
4. Should users normally rate every result in a scan, or only the few stocks they choose to investigate?
5. Are appraisals always private, or will teams eventually need shared lists and comments?
6. What outcome defines a successful `Winner`: absolute return, benchmark outperformance, no major drawdown, or a combination?
7. Which benchmark should ASX and US picks use, and should the benchmark depend on sector or market cap?
8. Will commercial users be allowed to create and save their own screening formulas?
9. Should the product launch as desktop-first, or must mobile appraisal be equally capable on day one?
10. Do you want the name `MoneyMaker` to remain the commercial brand, or is it currently a working title?

## Decision Process

Open the design lab, test Market and Analysis in each option, run the simulated fresh screen, change markets, select stocks and apply ratings. Use `Select this direction` when one concept is closest. The selection is stored only in the browser and does not alter production.

After a direction is chosen, the next design pass should combine the strongest details from the other concepts, define final typography and tokens, then implement the selected shell against the real Firebase APIs on a development branch.
