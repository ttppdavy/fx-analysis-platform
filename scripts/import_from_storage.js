const { createClient } = require('@supabase/supabase-js')
const XLSX = require('xlsx')

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
)

const OTHER_TRADE_SHEETS = [
  '外汇买卖',
  '远期',
  '掉期',
  '货币互换',
  '期权',
  '外币掉期',
  '期权组合',
  '掉期违约',
  '期权违约',
  '即远期违约',
  '柜台债现券买卖',
]

function normalizeDate(value) {
  if (value === null || value === undefined || value === '') return null
  if (typeof value === 'number') {
    const parsed = XLSX.SSF.parse_date_code(value)
    if (!parsed) return null
    const yyyy = String(parsed.y).padStart(4, '0')
    const mm = String(parsed.m).padStart(2, '0')
    const dd = String(parsed.d).padStart(2, '0')
    return `${yyyy}-${mm}-${dd}`
  }
  if (value instanceof Date && !isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10)
  }
  const str = String(value).trim()
  if (!str) return null
  const d = new Date(str.replace(/[./]/g, '-'))
  if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10)
  return str
}

function normalizeNumber(value) {
  if (value === null || value === undefined || value === '') return null
  if (typeof value === 'number') return Number.isFinite(value) ? value : null
  const str = String(value).replace(/,/g, '').trim()
  if (!str) return null
  const num = Number(str)
  return Number.isFinite(num) ? num : null
}

async function downloadWorkbook(bucket, path) {
  const { data, error } = await supabase.storage.from(bucket).download(path)
  if (error) throw error
  const arrayBuffer = await data.arrayBuffer()
  return XLSX.read(arrayBuffer, { type: 'array', cellDates: true })
}

async function insertInBatches(table, rows, size = 200) {
  for (let i = 0; i < rows.length; i += size) {
    const chunk = rows.slice(i, i + size)
    const { error } = await supabase.from(table).insert(chunk)
    if (error) throw error
  }
}

async function run() {
  const job = JSON.parse(process.env.IMPORT_JOB_JSON)

  const {
    fileType,       // trade / gold_lease / crm
    dataYear,       // 2025 / 2026
    tableName,      // 即汇通 / 其他交易表 / null
    bucket,         // trade-files / gold-files / crm-files
    storagePaths,   // array
    importMode,     // replace / append
  } = job

  const mode = importMode || 'replace'

  if (fileType === 'trade' && tableName === '即汇通') {
    if (mode === 'replace') {
      const { error: delErr } = await supabase
        .from('trade_raw_rows')
        .delete()
        .eq('data_year', dataYear)
        .eq('sheet_name', '即汇通')
      if (delErr) throw delErr
    }

    for (const storagePath of storagePaths) {
      const workbook = await downloadWorkbook(bucket, storagePath)
      const firstSheetName = workbook.SheetNames[0]
      const ws = workbook.Sheets[firstSheetName]
      const rows = XLSX.utils.sheet_to_json(ws, { defval: null })

      const payload = rows.map((row, idx) => ({
        data_year: dataYear,
        sheet_name: '即汇通',
        excel_row_num: idx + 2,
        row_data: row,
      }))

      if (payload.length) {
        await insertInBatches('trade_raw_rows', payload, 200)
      }
    }
  }

  if (fileType === 'trade' && tableName === '其他交易表') {
    const workbook = await downloadWorkbook(bucket, storagePaths[0])

    const { error: delErr } = await supabase
      .from('trade_raw_rows')
      .delete()
      .eq('data_year', dataYear)
      .in('sheet_name', OTHER_TRADE_SHEETS)
    if (delErr) throw delErr

    for (const sheetName of OTHER_TRADE_SHEETS) {
      if (!workbook.SheetNames.includes(sheetName)) continue
      const ws = workbook.Sheets[sheetName]
      const rows = XLSX.utils.sheet_to_json(ws, { defval: null })

      const payload = rows.map((row, idx) => ({
        data_year: dataYear,
        sheet_name: sheetName,
        excel_row_num: idx + 2,
        row_data: row,
      }))

      if (payload.length) {
        await insertInBatches('trade_raw_rows', payload, 200)
      }
    }
  }

  if (fileType === 'gold_lease') {
    const { error: delErr } = await supabase
      .from('gold_lease_trades')
      .delete()
      .eq('data_year', dataYear)
    if (delErr) throw delErr

    const workbook = await downloadWorkbook(bucket, storagePaths[0])
    const ws = workbook.Sheets[workbook.SheetNames[0]]
    const rows = XLSX.utils.sheet_to_json(ws, { defval: null })

    const payload = rows.map((r) => ({
      data_year: dataYear,
      业务编号: r['业务编号'] ?? null,
      客户名称: r['客户名称'] ?? null,
      一级分行: r['一级分行'] ?? null,
      租借品种: r['租借品种'] ?? null,
      货物属性: r['货物属性'] ?? null,
      租借重量_g: normalizeNumber(r['租借重量（g）'] ?? r['租借重量']),
      客户计费定盘价: normalizeNumber(r['客户计费定盘价']),
      同业定盘价: normalizeNumber(r['同业定盘价']),
      起息日: normalizeDate(r['起息日']),
      到期日: normalizeDate(r['到期日']),
      天数: normalizeNumber(r['天数']),
      应付租赁费率: normalizeNumber(r['应付租赁费率']),
      应收租赁费率: normalizeNumber(r['应收租赁费率']),
    }))

    if (payload.length) {
      await insertInBatches('gold_lease_trades', payload, 200)
    }
  }

  if (fileType === 'crm') {
    const { error: delErr } = await supabase
      .from('crm_customer_tags')
      .delete()
      .eq('data_year', dataYear)
    if (delErr) throw delErr

    const workbook = await downloadWorkbook(bucket, storagePaths[0])
    const ws = workbook.Sheets[workbook.SheetNames[0]]
    const rows = XLSX.utils.sheet_to_json(ws, { defval: null })

    const payload = rows
      .filter((r) => r['客户名称'])
      .map((r) => ({
        data_year: dataYear,
        客户名称: String(r['客户名称']).trim(),
        row_data: r,
      }))

    if (payload.length) {
      await insertInBatches('crm_customer_tags', payload, 200)
    }
  }

  console.log('导入完成')
}

run().catch((e) => {
  console.error(e)
  process.exit(1)
})