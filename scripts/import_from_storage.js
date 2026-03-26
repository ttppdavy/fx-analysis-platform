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

function log(...args) {
  console.log(new Date().toISOString(), ...args)
}

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
  log('开始下载文件', bucket, path)
  const { data, error } = await supabase.storage.from(bucket).download(path)
  if (error) throw error
  const arrayBuffer = await data.arrayBuffer()
  log('下载完成', bucket, path, 'bytes=', arrayBuffer.byteLength)
  return XLSX.read(arrayBuffer, { type: 'array', cellDates: true })
}

async function insertInBatches(table, rows, size = 50) {
  log(`开始插入 ${table}，总行数=${rows.length}，批次大小=${size}`)
  for (let i = 0; i < rows.length; i += size) {
    const chunk = rows.slice(i, i + size)
    log(`插入 ${table}：${i + 1} - ${Math.min(i + size, rows.length)}`)
    const { error } = await supabase.from(table).insert(chunk)
    if (error) throw error
  }
  log(`完成插入 ${table}`)
}

async function deleteTradeRowsBySheetInChunks(dataYear, sheetName, pageSize = 2000) {
  log(`开始分批删除 trade_raw_rows year=${dataYear}, sheet=${sheetName}`)

  while (true) {
    const { data, error } = await supabase
      .from('trade_raw_rows')
      .select('id')
      .eq('data_year', dataYear)
      .eq('sheet_name', sheetName)
      .limit(pageSize)

    if (error) throw error
    if (!data || data.length === 0) break

    const ids = data.map((x) => x.id)
    const { error: delErr } = await supabase
      .from('trade_raw_rows')
      .delete()
      .in('id', ids)

    if (delErr) throw delErr

    log(`已删除 ${ids.length} 条，sheet=${sheetName}`)
  }

  log(`删除完成 sheet=${sheetName}`)
}

async function deleteTradeRowsBySheetsInChunks(dataYear, sheetNames, pageSize = 2000) {
  for (const sheetName of sheetNames) {
    await deleteTradeRowsBySheetInChunks(dataYear, sheetName, pageSize)
  }
}

async function deleteGoldLeaseInChunks(dataYear, pageSize = 2000) {
  log(`开始分批删除 gold_lease_trades year=${dataYear}`)
  while (true) {
    const { data, error } = await supabase
      .from('gold_lease_trades')
      .select('id')
      .eq('data_year', dataYear)
      .limit(pageSize)

    if (error) throw error
    if (!data || data.length === 0) break

    const ids = data.map((x) => x.id)
    const { error: delErr } = await supabase
      .from('gold_lease_trades')
      .delete()
      .in('id', ids)

    if (delErr) throw delErr

    log(`已删除 gold_lease_trades ${ids.length} 条`)
  }
  log(`删除 gold_lease_trades 完成`)
}

async function deleteCrmInChunks(dataYear, pageSize = 2000) {
  log(`开始分批删除 crm_customer_tags year=${dataYear}`)
  while (true) {
    const { data, error } = await supabase
      .from('crm_customer_tags')
      .select('id')
      .eq('data_year', dataYear)
      .limit(pageSize)

    if (error) throw error
    if (!data || data.length === 0) break

    const ids = data.map((x) => x.id)
    const { error: delErr } = await supabase
      .from('crm_customer_tags')
      .delete()
      .in('id', ids)

    if (delErr) throw delErr

    log(`已删除 crm_customer_tags ${ids.length} 条`)
  }
  log(`删除 crm_customer_tags 完成`)
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

  log('开始导入任务', JSON.stringify(job))

  if (fileType === 'trade' && tableName === '即汇通') {
    if (mode === 'replace') {
      await deleteTradeRowsBySheetInChunks(dataYear, '即汇通', 1000)
    }

    for (const storagePath of storagePaths) {
      log('处理即汇通文件', storagePath)
      const workbook = await downloadWorkbook(bucket, storagePath)
      const firstSheetName = workbook.SheetNames[0]
      const ws = workbook.Sheets[firstSheetName]
      const rows = XLSX.utils.sheet_to_json(ws, { defval: null })

      log(`即汇通 ${storagePath} 读取完成，rows=${rows.length}`)

      const payload = rows.map((row, idx) => ({
        data_year: dataYear,
        sheet_name: '即汇通',
        excel_row_num: idx + 2,
        row_data: row,
      }))

      if (payload.length) {
        await insertInBatches('trade_raw_rows', payload, 50)
      }
    }
  }

  if (fileType === 'trade' && tableName === '其他交易表') {
    const storagePath = storagePaths[0]
    log('处理其他交易表文件', storagePath)

    const workbook = await downloadWorkbook(bucket, storagePath)

    await deleteTradeRowsBySheetsInChunks(dataYear, OTHER_TRADE_SHEETS, 1000)

    for (const sheetName of OTHER_TRADE_SHEETS) {
      if (!workbook.SheetNames.includes(sheetName)) {
        log(`未找到 sheet，跳过：${sheetName}`)
        continue
      }

      const ws = workbook.Sheets[sheetName]
      const rows = XLSX.utils.sheet_to_json(ws, { defval: null })

      log(`sheet=${sheetName} 读取完成，rows=${rows.length}`)

      const payload = rows.map((row, idx) => ({
        data_year: dataYear,
        sheet_name: sheetName,
        excel_row_num: idx + 2,
        row_data: row,
      }))

      if (payload.length) {
        await insertInBatches('trade_raw_rows', payload, 50)
      }
    }
  }

  if (fileType === 'gold_lease') {
    await deleteGoldLeaseInChunks(dataYear, 1000)

    const workbook = await downloadWorkbook(bucket, storagePaths[0])
    const ws = workbook.Sheets[workbook.SheetNames[0]]
    const rows = XLSX.utils.sheet_to_json(ws, { defval: null })

    log(`黄金租赁读取完成，rows=${rows.length}`)

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
      await insertInBatches('gold_lease_trades', payload, 50)
    }
  }

  if (fileType === 'crm') {
    await deleteCrmInChunks(dataYear, 1000)

    const workbook = await downloadWorkbook(bucket, storagePaths[0])
    const ws = workbook.Sheets[workbook.SheetNames[0]]
    const rows = XLSX.utils.sheet_to_json(ws, { defval: null })

    log(`CRM读取完成，rows=${rows.length}`)

    const payload = rows
      .filter((r) => r['客户名称'])
      .map((r) => ({
        data_year: dataYear,
        客户名称: String(r['客户名称']).trim(),
        row_data: r,
      }))

    if (payload.length) {
      await insertInBatches('crm_customer_tags', payload, 50)
    }
  }

  log('导入完成')
}

run().catch((e) => {
  console.error(e)
  process.exit(1)
})