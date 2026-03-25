import { NextResponse } from 'next/server'
import { supabaseAdmin } from '@/lib/supabase-server'
import * as XLSX from 'xlsx'

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

type ImportBody = {
  fileType: 'trade' | 'gold_lease' | 'crm'
  dataYear: number
  tableName?: string | null
  storagePaths: string[]
  sourceFilenames: string[]
  bucket: string
}

function normalizeDate(value: any): string | null {
  if (value === null || value === undefined || value === '') return null

  if (typeof value === 'number') {
    const parsed = XLSX.SSF.parse_date_code(value)
    if (!parsed) return null
    const yyyy = parsed.y.toString().padStart(4, '0')
    const mm = parsed.m.toString().padStart(2, '0')
    const dd = parsed.d.toString().padStart(2, '0')
    return `${yyyy}-${mm}-${dd}`
  }

  if (value instanceof Date && !isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10)
  }

  const str = String(value).trim()
  if (!str) return null

  const normalized = str.replace(/[./]/g, '-')
  const d = new Date(normalized)
  if (!isNaN(d.getTime())) {
    return d.toISOString().slice(0, 10)
  }

  return normalized
}

function normalizeNumber(value: any): number | null {
  if (value === null || value === undefined || value === '') return null
  if (typeof value === 'number') return Number.isFinite(value) ? value : null

  const str = String(value).replace(/,/g, '').trim()
  if (!str) return null
  const num = Number(str)
  return Number.isFinite(num) ? num : null
}

async function createBatch(body: ImportBody) {
  const sourceFilenameText = Array.isArray(body.sourceFilenames)
    ? body.sourceFilenames.join(' | ')
    : ''

  const storagePathText = Array.isArray(body.storagePaths)
    ? body.storagePaths.join(' | ')
    : ''

  const { data, error } = await supabaseAdmin
    .from('etl_batches')
    .insert({
      file_type: body.fileType,
      data_year: body.dataYear,
      table_name: body.tableName ?? null,
      source_filename: sourceFilenameText,
      storage_path: storagePathText,
      status: 'processing',
    })
    .select()
    .single()

  if (error) throw error
  return data
}

async function finishBatchSuccess(batchId: number, rowCount: number) {
  const { error } = await supabaseAdmin
    .from('etl_batches')
    .update({
      status: 'success',
      row_count: rowCount,
      finished_at: new Date().toISOString(),
    })
    .eq('id', batchId)

  if (error) throw error
}

async function finishBatchFailed(batchId: number, errorMessage: string) {
  await supabaseAdmin
    .from('etl_batches')
    .update({
      status: 'failed',
      error_message: errorMessage,
      finished_at: new Date().toISOString(),
    })
    .eq('id', batchId)
}

async function downloadWorkbook(bucket: string, storagePath: string) {
  const { data: fileData, error } = await supabaseAdmin
    .storage
    .from(bucket)
    .download(storagePath)

  if (error) throw error

  const arrayBuffer = await fileData.arrayBuffer()
  return XLSX.read(arrayBuffer, {
    type: 'array',
    cellDates: true,
  })
}

async function importJiHuiTong(
  batchId: number,
  dataYear: number,
  bucket: string,
  storagePaths: string[],
) {
  let rowCount = 0

  const { error: deleteError } = await supabaseAdmin
    .from('trade_raw_rows')
    .delete()
    .eq('data_year', dataYear)
    .eq('sheet_name', '即汇通')

  if (deleteError) throw deleteError

  for (const storagePath of storagePaths) {
    const workbook = await downloadWorkbook(bucket, storagePath)
    const firstSheetName = workbook.SheetNames[0]
    if (!firstSheetName) continue

    const ws = workbook.Sheets[firstSheetName]
    const rows = XLSX.utils.sheet_to_json(ws, { defval: null })

    if (!rows.length) continue

    const payload = rows.map((row: any, idx: number) => ({
      batch_id: batchId,
      data_year: dataYear,
      sheet_name: '即汇通',
      excel_row_num: idx + 2,
      row_data: row,
    }))

    const { error } = await supabaseAdmin.from('trade_raw_rows').insert(payload)
    if (error) throw error

    rowCount += payload.length
  }

  return rowCount
}

async function importOtherTrades(
  batchId: number,
  dataYear: number,
  bucket: string,
  storagePath: string,
) {
  let rowCount = 0

  const workbook = await downloadWorkbook(bucket, storagePath)

  const { error: deleteError } = await supabaseAdmin
    .from('trade_raw_rows')
    .delete()
    .eq('data_year', dataYear)
    .in('sheet_name', OTHER_TRADE_SHEETS)

  if (deleteError) throw deleteError

  for (const sheetName of OTHER_TRADE_SHEETS) {
    if (!workbook.SheetNames.includes(sheetName)) continue

    const ws = workbook.Sheets[sheetName]
    const rows = XLSX.utils.sheet_to_json(ws, { defval: null })

    if (!rows.length) continue

    const payload = rows.map((row: any, idx: number) => ({
      batch_id: batchId,
      data_year: dataYear,
      sheet_name: sheetName,
      excel_row_num: idx + 2,
      row_data: row,
    }))

    const { error } = await supabaseAdmin.from('trade_raw_rows').insert(payload)
    if (error) throw error

    rowCount += payload.length
  }

  return rowCount
}

async function importGoldLease(
  batchId: number,
  dataYear: number,
  bucket: string,
  storagePath: string,
) {
  let rowCount = 0

  const { error: deleteError } = await supabaseAdmin
    .from('gold_lease_trades')
    .delete()
    .eq('data_year', dataYear)

  if (deleteError) throw deleteError

  const workbook = await downloadWorkbook(bucket, storagePath)
  const firstSheetName = workbook.SheetNames[0]
  if (!firstSheetName) return 0

  const ws = workbook.Sheets[firstSheetName]
  const rows = XLSX.utils.sheet_to_json(ws, { defval: null }) as any[]

  if (!rows.length) return 0

  const payload = rows.map((r) => ({
    batch_id: batchId,
    data_year: dataYear,
    业务编号: r['业务编号'] ?? null,
    客户名称: r['客户名称'] ?? null,
    一级分行: r['一级分行'] ?? null,
    租借品种: r['租借品种'] ?? null,
    货物属性: r['货物属性'] ?? null,
    租借重量_g: normalizeNumber(r['租借重量（g）'] ?? r['租借重量_g'] ?? r['租借重量']),
    客户计费定盘价: normalizeNumber(r['客户计费定盘价']),
    同业定盘价: normalizeNumber(r['同业定盘价']),
    起息日: normalizeDate(r['起息日']),
    到期日: normalizeDate(r['到期日']),
    天数: normalizeNumber(r['天数']),
    应付租赁费率: normalizeNumber(r['应付租赁费率']),
    应收租赁费率: normalizeNumber(r['应收租赁费率']),
  }))

  const { error } = await supabaseAdmin.from('gold_lease_trades').insert(payload)
  if (error) throw error

  rowCount += payload.length
  return rowCount
}

async function importCrm(
  batchId: number,
  dataYear: number,
  bucket: string,
  storagePath: string,
) {
  let rowCount = 0

  const { error: deleteError } = await supabaseAdmin
    .from('crm_customer_tags')
    .delete()
    .eq('data_year', dataYear)

  if (deleteError) throw deleteError

  const workbook = await downloadWorkbook(bucket, storagePath)
  const firstSheetName = workbook.SheetNames[0]
  if (!firstSheetName) return 0

  const ws = workbook.Sheets[firstSheetName]
  const rows = XLSX.utils.sheet_to_json(ws, { defval: null }) as any[]

  if (!rows.length) return 0

  const payload = rows
    .filter((r) => r['客户名称'])
    .map((r) => ({
      batch_id: batchId,
      data_year: dataYear,
      客户名称: String(r['客户名称']).trim(),
      row_data: r,
    }))

  if (!payload.length) return 0

  const { error } = await supabaseAdmin.from('crm_customer_tags').insert(payload)
  if (error) throw error

  rowCount += payload.length
  return rowCount
}

export async function POST(req: Request) {
  let batchId: number | null = null

  try {
    const body = (await req.json()) as ImportBody
    const { fileType, dataYear, tableName, storagePaths, bucket } = body

    if (!fileType) {
      return NextResponse.json({ message: '缺少 fileType' }, { status: 400 })
    }

    if (!dataYear) {
      return NextResponse.json({ message: '缺少 dataYear' }, { status: 400 })
    }

    if (!bucket) {
      return NextResponse.json({ message: '缺少 bucket' }, { status: 400 })
    }

    if (!Array.isArray(storagePaths) || storagePaths.length === 0) {
      return NextResponse.json({ message: '缺少 storagePaths' }, { status: 400 })
    }

    if (fileType === 'trade') {
      if (!tableName) {
        return NextResponse.json({ message: '交易表必须选择表名' }, { status: 400 })
      }

      if (tableName === '即汇通' && storagePaths.length > 3) {
        return NextResponse.json({ message: '即汇通一次最多上传 3 个文件' }, { status: 400 })
      }

      if (tableName === '其他交易表' && storagePaths.length !== 1) {
        return NextResponse.json({ message: '其他交易表一次只能上传 1 个文件' }, { status: 400 })
      }
    }

    if ((fileType === 'gold_lease' || fileType === 'crm') && storagePaths.length !== 1) {
      return NextResponse.json({ message: '黄金租赁和 CRM 一次只能上传 1 个文件' }, { status: 400 })
    }

    const batch = await createBatch(body)
    batchId = batch.id

    let rowCount = 0

    if (fileType === 'trade') {
      if (tableName === '即汇通') {
        rowCount = await importJiHuiTong(batch.id, dataYear, bucket, storagePaths)
      } else if (tableName === '其他交易表') {
        rowCount = await importOtherTrades(batch.id, dataYear, bucket, storagePaths[0])
      } else {
        throw new Error('不支持的交易表类型')
      }
    }

    if (fileType === 'gold_lease') {
      rowCount = await importGoldLease(batch.id, dataYear, bucket, storagePaths[0])
    }

    if (fileType === 'crm') {
      rowCount = await importCrm(batch.id, dataYear, bucket, storagePaths[0])
    }

    await finishBatchSuccess(batch.id, rowCount)

    return NextResponse.json({
      message: `导入成功，${rowCount} 行`,
    })
  } catch (e: any) {
    const errorMessage = e?.message || '导入失败'

    if (batchId) {
      await finishBatchFailed(batchId, errorMessage)
    }

    return NextResponse.json(
      { message: errorMessage },
      { status: 500 },
    )
  }
}