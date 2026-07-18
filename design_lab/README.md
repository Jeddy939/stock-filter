# MoneyMaker Design Lab

This is an isolated, static product-design prototype. It does not call the MoneyMaker API, alter production data or deploy through Firebase Hosting.

## Open locally

From the repository root:

```powershell
py -3 -m http.server 8765 --bind 127.0.0.1
```

Then open:

- Design chooser: <http://127.0.0.1:8765/design_lab/>
- Research Workbench: <http://127.0.0.1:8765/design_lab/?concept=workbench>
- Investment Ledger: <http://127.0.0.1:8765/design_lab/?concept=ledger>
- Signal Review: <http://127.0.0.1:8765/design_lab/?concept=review>

Use the top concept picker to switch directions. `Select this direction` stores the current choice in browser local storage so it survives a refresh. It does not change application code or production configuration.

The mock interactions include:

- Market and Analysis navigation.
- ASX and US switching.
- Stock selection and chart updates.
- Appraisal controls and private-note entry.
- Ticker/company search.
- Filter disclosure.
- A simulated fresh-screen operation with progress stages.
- Responsive layouts for desktop and mobile review.

See [the commercial UI audit](../docs/COMMERCIAL_UI_AUDIT.md) for the findings, reference products and decision questions behind the concepts.
