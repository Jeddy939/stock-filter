const SHEET_NAME = 'Ratings';

const HEADERS = [
  'event_id',
  'event_at_utc',
  'action',
  'rated_by',
  'market',
  'ticker',
  'label',
  'note',
  'scan_id',
  'scan_created_at_utc',
  'rank',
  'signal_date',
  'close_price',
  'market_cap',
  'avg_volume',
  'volume_ratio',
  'sector',
  'industry',
  'provider',
  'query',
  'cache_file',
  'yahoo_url',
  'raw_json',
];

function doPost(e) {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);

  try {
    const payload = JSON.parse((e.postData && e.postData.contents) || '{}');
    const expectedSecret = PropertiesService.getScriptProperties().getProperty('MONEYMAKER_SECRET');
    if (expectedSecret && payload.secret !== expectedSecret) {
      return jsonResponse({ ok: false, error: 'bad secret' });
    }

    const sheet = getRatingsSheet_();
    const eventId = String(payload.event_id || '');
    if (!eventId) {
      return jsonResponse({ ok: false, error: 'missing event_id' });
    }

    if (hasEvent_(sheet, eventId)) {
      return jsonResponse({ ok: true, duplicate: true, event_id: eventId });
    }

    sheet.appendRow([
      eventId,
      payload.event_at_utc || '',
      payload.action || '',
      payload.rated_by || '',
      payload.market || '',
      payload.ticker || '',
      payload.label || '',
      payload.note || '',
      payload.scan_id || '',
      payload.scan_created_at_utc || '',
      payload.rank || '',
      payload.signal_date || '',
      payload.close_price || '',
      payload.market_cap || '',
      payload.avg_volume || '',
      payload.volume_ratio || '',
      payload.sector || '',
      payload.industry || '',
      payload.provider || '',
      payload.query || '',
      payload.cache_file || '',
      payload.yahoo_url || '',
      JSON.stringify(payload),
    ]);

    return jsonResponse({ ok: true, event_id: eventId });
  } catch (err) {
    return jsonResponse({ ok: false, error: String(err) });
  } finally {
    lock.releaseLock();
  }
}

function getRatingsSheet_() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = spreadsheet.getSheetByName(SHEET_NAME);
  if (!sheet) {
    sheet = spreadsheet.insertSheet(SHEET_NAME);
  }

  if (sheet.getLastRow() === 0) {
    sheet.appendRow(HEADERS);
    sheet.setFrozenRows(1);
  }

  return sheet;
}

function hasEvent_(sheet, eventId) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return false;
  }
  const finder = sheet.getRange(2, 1, lastRow - 1, 1).createTextFinder(eventId);
  finder.matchEntireCell(true);
  return Boolean(finder.findNext());
}

function jsonResponse(payload) {
  return ContentService
    .createTextOutput(JSON.stringify(payload))
    .setMimeType(ContentService.MimeType.JSON);
}
